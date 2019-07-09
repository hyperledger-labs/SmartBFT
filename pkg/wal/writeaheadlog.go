// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

const (
	walFileSuffix   string = ".wal"
	walFileTemplate        = "%016x" + walFileSuffix

	walFilePermPrivateRW os.FileMode = 0600
	walDirPermPrivateRWX os.FileMode = 0700

	recordHeaderSize int    = 8
	recordLengthMask uint64 = 0x00000000FFFFFFFF
	recordCRCMask    uint64 = recordLengthMask << 32

	walCRCSeed uint32 = 0xDEED0001

	FileSizeBytesDefault   int64 = 64 * 1024 * 1024 // 64MB
	BufferSizeBytesDefault int64 = 1024 * 1024      // 1MB
)

var (
	ErrCRC       = errors.New("wal: crc verification failed")
	ErrWriteOnly = errors.New("wal: in WRITE mode")
	ErrReadOnly  = errors.New("wal: in READ mode")

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type LogRecordLength uint32
type LogRecordCRC uint32

// LogRecordHeader contains the LogRecordLength (lower 32 bits) and LogRecordCRC (upper 32 bits).
type LogRecordHeader uint64

// WriteAheadLogFile is a simple implementation of a write ahead log (WAL).
//
// The WAL is composed of a sequence of frames. Each frame contains:
// - a header (uint64)
// - data: a record of type LogRecord, marshaled to bytes, and padded with zeros to 8B boundary.
//
// The 64 bit header is made of two parts:
// - length of the marshaled LogRecord (not including pad bytes), in the lower 32 bits.
// - a crc32 of the data: marshaled record bytes + pad bytes, in the upper 32 bits.
//
// The WAL is written to a sequence of files: <index>.wal, where index uint64=1,2,3...; represented in fixed-width
// hex format, e.g. 0000000000000001.wal
//
// The WAL has two modes: append, and read.
//
// When a WAL is first created, it is in append mode.
// When an existing WAL is opened, it is in read mode, and will change to append mode only after ReadAll() is invoked.

// In append mode the WAL can accept Append() and TruncateTo() calls.
// The WAL must be closed after use to release all resources.
//
type WriteAheadLogFile struct {
	dirName string
	options *Options

	logger api.Logger

	mutex         sync.Mutex
	dirFile       *os.File
	index         uint64
	logFile       *os.File
	headerBuff    []byte
	dataBuff      *proto.Buffer
	crc           uint32
	readMode      bool
	truncateIndex uint64
	activeIndexes []uint64
}

type Options struct {
	FileSizeBytes   int64
	BufferSizeBytes int64
}

// DefaultOptions returns the set of default options.
func DefaultOptions() *Options {
	return &Options{
		FileSizeBytes:   FileSizeBytesDefault,
		BufferSizeBytes: BufferSizeBytesDefault,
	}
}

// Create will create a new WAL, if it does not exist, or an error if it already exists.
//
// logger: reference to a Logger implementation.
// dirPath: directory path of the WAL.
// options: a structure containing Options, or nil, for default options.
//
// return: pointer to a WAL, or an error
func Create(logger api.Logger, dirPath string, options *Options) (*WriteAheadLogFile, error) {
	if logger == nil {
		return nil, errors.New("wal: logger is nil")
	}
	if !dirEmpty(dirPath) {
		return nil, fmt.Errorf("wal: directory not empty: %s", dirPath)
	}
	opt := DefaultOptions()
	if options != nil {
		opt = options
	}

	//TODO BACKLOG: create the directory & file atomically by creation in a temp dir and renaming
	cleanDirName := filepath.Clean(dirPath)
	err := dirCreate(cleanDirName)
	if err != nil {
		return nil, fmt.Errorf("wal: could not create directory: %s; error: %s", dirPath, err)
	}

	wal := &WriteAheadLogFile{
		dirName:       cleanDirName,
		options:       opt,
		logger:        logger,
		index:         1,
		headerBuff:    make([]byte, 8),
		dataBuff:      proto.NewBuffer(make([]byte, opt.BufferSizeBytes)),
		crc:           walCRCSeed,
		truncateIndex: 1,
		activeIndexes: []uint64{1},
	}

	wal.dirFile, err = os.Open(cleanDirName)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("wal: could not open directory: %s; error: %s", dirPath, err)
	}

	fileName := fmt.Sprintf(walFileTemplate, uint64(1))
	wal.logFile, err = os.OpenFile(filepath.Join(cleanDirName, fileName), os.O_CREATE|os.O_WRONLY, walFilePermPrivateRW)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("wal: could not open file: %s; error: %s", fileName, err)
	}

	err = wal.saveCRC()
	if err != nil {
		wal.Close()
		return nil, err
	}

	wal.logger.Infof("Write-Ahead-Log created successfully, mode: WRITE, dir: %s", wal.dirName)
	return wal, nil
}

// Open will open an existing WAL, if it exists, or an error if it does not exist.
//
// After opening, the WAL is in read mode, and expects a call to ReadAll(). An attempt to write
// (e.g. Append, TruncateTo) will result in an error.
//
// logger: reference to a Logger implementation.
// dirPath: directory path of the WAL.
// options: a structure containing Options, or nil, for default options.
//
// return: pointer to a WAL, or an error
func Open(logger api.Logger, dirPath string, options *Options) (*WriteAheadLogFile, error) {
	if logger == nil {
		return nil, errors.New("wal: logger is nil")
	}

	walNames, err := dirReadWalNames(dirPath)
	if err != nil {
		return nil, err
	}
	if len(walNames) == 0 {
		return nil, os.ErrNotExist
	}

	logger.Infof("Write-Ahead-Log discovered %d wal files: %s", len(walNames), strings.Join(walNames, ", "))

	opt := DefaultOptions()
	if options != nil {
		opt = options
	}

	cleanDirName := filepath.Clean(dirPath)

	wal := &WriteAheadLogFile{
		dirName:    cleanDirName,
		options:    opt,
		logger:     logger,
		headerBuff: make([]byte, 8),
		dataBuff:   proto.NewBuffer(make([]byte, opt.BufferSizeBytes)),
		readMode:   true,
	}

	wal.dirFile, err = os.Open(cleanDirName)
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("wal: could not open directory: %s; error: %s", dirPath, err)
	}

	// After the check we have an increasing, continuous sequence, with valid CRC-Anchors in each file.
	wal.activeIndexes, err = checkWalFiles(logger, dirPath, walNames)
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	fileName := walNames[0] //first valid file
	wal.index, err = parseWalFileName(fileName)
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	wal.logger.Infof("Write-Ahead-Log opened successfully, mode: READ, dir: %s", wal.dirName)

	return wal, nil
}

// Repair tries to repair the last file of a WAL, in case the last item is corrupted due to a failure in the middle of
// the last write. It does so by dropping the last corrupt item.
//
// logger: reference to a Logger implementation.
// dirPath: directory path of the WAL.
// return: an error if repair was not successful.
func Repair(logger api.Logger, dirPath string) error {
	//TODO BACKLOG
	return errors.New("not implemented yet")
}

// Close the files and directory of the WAL, and release all resources.
func (w *WriteAheadLogFile) Close() error {
	var errF, errD error

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.logFile != nil {
		if errF = w.truncateAndCloseLogFile(); errF != nil {
			w.logger.Errorf("failed to properly close log file %s; error: %s", w.logFile.Name(), errF)
		}
		w.logFile = nil
	}

	w.dataBuff = nil
	w.headerBuff = nil

	if w.dirFile != nil {
		if errD = w.dirFile.Close(); errD != nil {
			w.logger.Errorf("failed to properly close directory %s; error: %s", w.dirName, errD)
		}
		w.dirFile = nil
	}

	//return the first error
	switch {
	case errF != nil:
		return errF
	default:
		return errD
	}
}

// CRC returns the last CRC written to the log file.
func (w *WriteAheadLogFile) CRC() uint32 {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.crc
}

// TruncateTo appends a control record in which the TruncateTo flag is true.
// This marks that every record prior to this one can be safely truncated from the log.
func (w *WriteAheadLogFile) TruncateTo() error {
	record := &protos.LogRecord{
		Type:       protos.LogRecord_CONTROL,
		TruncateTo: true,
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.append(record)
}

// Append a data item to the end of the WAL and indicate whether this entry is a truncation point.
//
// The data item will be added to the log, and internally marked with a flag that indicates whether
// it is a truncation point. The log implementation may truncate all preceding data items, not including this one.
//
// data: the data to be appended to the log. Cannot be nil or empty.
// truncateTo: whether all records preceding this one, but not including it, can be truncated from the log.
func (w *WriteAheadLogFile) Append(data []byte, truncateTo bool) error {
	if len(data) == 0 {
		return errors.New("data is nil or empty")
	}

	record := &protos.LogRecord{
		Type:       protos.LogRecord_ENTRY,
		TruncateTo: truncateTo,
		Data:       data,
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.append(record)
}

func (w *WriteAheadLogFile) append(record *protos.LogRecord) error {
	if w.dirFile == nil {
		return os.ErrClosed
	}
	if w.readMode {
		return ErrReadOnly
	}

	w.dataBuff.Reset()
	err := w.dataBuff.Marshal(record)
	if err != nil {
		return fmt.Errorf("wal: failed to marshal to data buffer: %s", err)
	}

	payloadBuff := w.dataBuff.Bytes()
	recordLength := len(payloadBuff)
	if (uint64(recordLength) & recordCRCMask) != 0 {
		return fmt.Errorf("wal: record too big, length does not fit in uint32: %d", recordLength)
	}
	padSize, padBytes := getPadBytes(recordLength)
	if padSize != 0 {
		payloadBuff = append(payloadBuff, padBytes...)
	}
	dataCRC := crc32.Update(w.crc, crcTable, payloadBuff)
	header := uint64(recordLength) | (uint64(dataCRC) << 32)

	binary.LittleEndian.PutUint64(w.headerBuff, header)
	nh, err := w.logFile.Write(w.headerBuff)
	if err != nil {
		return fmt.Errorf("wal: failed to write header bytes: %s", err)
	}

	np, err := w.logFile.Write(payloadBuff)
	if err != nil {
		return fmt.Errorf("wal: failed to write payload bytes: %s", err)
	}

	err = w.logFile.Sync()
	if err != nil {
		return fmt.Errorf("wal: failed to Sync log file: %s", err)
	}
	w.crc = dataCRC

	offset, err := w.logFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("wal: failed to get offset from log file: %s", err)
	}

	if record.TruncateTo {
		w.truncateIndex = w.index
	}
	w.logger.Debugf("LogRecord appended successfully: total size=%d, recordLength=%d, dataCRC=%08X; file=%s, new-offset=%d",
		(nh + np), recordLength, dataCRC, w.logFile.Name(), offset)

	//Switch files if this or the next record (minimal size is 16B) cause overflow
	if offset > w.options.FileSizeBytes-16 {
		err = w.switchFiles()
		if err != nil {
			return fmt.Errorf("wal: failed to switch log files: %s", err)
		}
	}

	return nil
}

// ReadAll the data items from the latest truncation point to the end of the log.
// This method can be called only at the beginning of the WAL lifecycle, right after Open().
// After a successful invocation the WAL moves to write mode, and is ready to Append().
//
// In case of failure:
//  - an error of type io.ErrUnexpectedEOF	is returned when the WAL can possibly be repaired by truncating the last
//    log file after the last good record.
//  - all other errors indicate that the WAL is either
//  	- is closed, or
//  	- is in write mode, or
//  	- is corrupted beyond the simple repair measure described above.
func (w *WriteAheadLogFile) ReadAll() ([][]byte, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.dirFile == nil {
		return nil, os.ErrClosed
	}

	if !w.readMode {
		return nil, ErrWriteOnly
	}

	var items = make([][]byte, 0)
	var lastIndex = w.activeIndexes[len(w.activeIndexes)-1]

FileLoop:
	for i, index := range w.activeIndexes {
		w.index = index
		//This should not fail, we check the files earlier, when we Open() the WAL.
		r, err := NewLogRecordReader(w.logger, filepath.Join(w.dirName, fmt.Sprintf(walFileTemplate, w.index)))
		if err != nil {
			return nil, err
		}
		if (i != 0) && (r.CRC() != w.crc) {
			return nil, ErrCRC
		}

		var readErr error
	ReadLoop:
		for i := 1; ; i++ {
			var rec *protos.LogRecord
			rec, readErr = r.Read()
			if readErr != nil {
				w.logger.Debugf("Read error, file: %s; error: %s", r.fileName, readErr)
				r.Close()
				break ReadLoop
			}

			if rec.TruncateTo {
				items = items[0:0]
				w.truncateIndex = w.index
			}

			if rec.Type == protos.LogRecord_ENTRY {
				items = append(items, rec.Data)
			}

			w.logger.Debugf("Read record #%d, file: %s", i, r.fileName)
		}

		if readErr == io.EOF {
			w.logger.Debugf("Reached EOF, finished reading file: %s; CRC: %08X", r.fileName, r.CRC())
			w.crc = r.CRC()
			continue FileLoop
		}

		if index == lastIndex && (readErr == io.ErrUnexpectedEOF || readErr == ErrCRC) {
			w.logger.Warnf("Received an error in the last file, this can possibly be repaired; file: %s; error: %s", r.fileName, err)
			// This error is returned when the WAL can possibly be repaired
			return nil, io.ErrUnexpectedEOF
		}

		if readErr != nil {
			w.logger.Warnf("Failed reading file: %s; error: %s", r.fileName, err)
			return nil, fmt.Errorf("failed reading wal: %s", err)
		}
	}

	w.logger.Debugf("Read %d items", len(items))

	// move to write mode on a new file.
	if err := w.switchFiles(); err != nil {
		w.logger.Errorf("Failed to switch files: %s", err)
		return nil, err
	}
	w.readMode = false

	return items, nil
}

// truncateAndCloseLogFile when we orderly close a writable log file we truncate it. This way, reading it in ReadAll()
// ends with a io.EOF error after the last record.
func (w *WriteAheadLogFile) truncateAndCloseLogFile() error {
	var err error

	if !w.readMode {
		offset, err := w.logFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		if err = w.logFile.Truncate(offset); err != nil {
			return err
		}
		if err = w.logFile.Sync(); err != nil {
			return err
		}

		w.logger.Debugf("Truncated & Sync'ed log file: %s", w.logFile.Name())
	}

	if err = w.logFile.Close(); err != nil {
		w.logger.Errorf("Failed to close log file: %s; error: %s", w.logFile.Name(), err)
		return err
	}

	w.logger.Debugf("Closed log file: %s", w.logFile.Name())

	return nil
}

func (w *WriteAheadLogFile) switchFiles() error {
	var err error

	if !w.readMode {
		if err = w.truncateAndCloseLogFile(); err != nil {
			w.logger.Errorf("Failed to truncateAndCloseLogFile: %s", err)
			return err
		}
	}

	w.index++
	nextFileName := fmt.Sprintf(walFileTemplate, w.index)
	nextFilePath := filepath.Join(w.dirFile.Name(), nextFileName)
	w.logger.Debugf("Preparing next log file: %s", nextFilePath)

	//TODO BACKLOG: prepare a pre-allocated file in advance, and get it here.
	w.logFile, err = os.OpenFile(nextFilePath, os.O_CREATE|os.O_WRONLY, walFilePermPrivateRW)
	if err != nil {
		w.logger.Errorf("Failed to OpenFile: %s", err)
		return err
	}
	_, err = w.logFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = w.saveCRC()
	if err != nil {
		return err
	}

	w.activeIndexes = append(w.activeIndexes, w.index)
	w.logger.Debugf("Successfully switched to log file: %s", w.logFile.Name())
	w.logger.Debugf("Number of files: %d, indexes: %v", len(w.activeIndexes), w.activeIndexes)

	return nil
}

// saveCRC saves the current CRC followed by a CRC_ANCHOR record.
func (w *WriteAheadLogFile) saveCRC() error {
	anchorRecord := &protos.LogRecord{Type: protos.LogRecord_CRC_ANCHOR, TruncateTo: false}
	b, err := proto.Marshal(anchorRecord)
	recordLength := len(b)
	padSize, padBytes := getPadBytes(recordLength)
	if padSize != 0 {
		b = append(b, padBytes...)
	}

	header := uint64(recordLength) | (uint64(w.crc) << 32)
	binary.LittleEndian.PutUint64(w.headerBuff, header)
	offset, err := w.logFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	nh, err := w.logFile.Write(w.headerBuff)
	if err != nil {
		return fmt.Errorf("wal: failed to write crc-anchor header bytes: %s", err)
	}

	nb, err := w.logFile.Write(b)
	if err != nil {
		return fmt.Errorf("wal: failed to write crc-anchor payload bytes: %s", err)
	}
	err = w.logFile.Sync()
	if err != nil {
		return fmt.Errorf("wal: failed to Sync: %s", err)
	}

	w.logger.Debugf("CRC-Anchor %08X written to file: %s, at offset %d, size=%d", w.crc, w.logFile.Name(), offset, nh+nb)

	return nil
}

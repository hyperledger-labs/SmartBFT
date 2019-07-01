// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/SmartBFT-Go/consensus/pkg/api"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"hash/crc32"
	"io"
	"os"
)

type LogRecordReader struct {
	fileName string
	logger api.Logger
	logFile  *os.File
	crc      uint32
}

func NewLogRecordReader(logger api.Logger, fileName string) (*LogRecordReader, error) {
	if logger == nil {
		return nil, errors.New("logger is nil")
	}

	r := &LogRecordReader{
		fileName: fileName,
		logger: logger,
	}

	var err error
	r.logFile, err = os.Open(fileName)
	if err != nil {
		return nil, err
	}

	_, err = r.logFile.Seek(0, io.SeekStart)
	if err != nil {
		_ = r.Close()
		return nil, err
	}

	//read the CRC-Anchor, the first record of every file
	recLen, crc, err := r.readHeader()
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	padSize := getPadSize(int(recLen))
	payload, err := r.readPayload(int(recLen) + padSize)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	var record = &protos.LogRecord{}
	err = proto.Unmarshal(payload[:recLen], record)
	if err != nil {
		_ = r.Close()
		return nil, err
	}

	if record.Type != protos.LogRecord_CRC_ANCHOR {
		_ = r.Close()
		return nil, fmt.Errorf("failed reading CRC-Anchor from log file: %s", fileName)
	}
	r.crc = crc

	r.logger.Debugf("Initialized reader: CRC-Anchor: %08X, file: %s", r.crc, r.fileName)

	return r, nil
}

func (r *LogRecordReader) Close() error {
	var err error
	if r.logFile != nil {
		err = r.logFile.Close()
	}
	r.logger = nil
	r.logFile = nil
	return err
}

func(r *LogRecordReader) CRC() uint32 {
	return r.crc
}

func (r *LogRecordReader) Read() (*protos.LogRecord, error) {
	recLen, crc, err := r.readHeader()
	if err != nil {
		return nil, err
	}
	padSize := getPadSize(int(recLen))
	payload, err := r.readPayload(int(recLen) + padSize)
	if err != nil {
		return nil, err
	}
	var record = &protos.LogRecord{}
	err = proto.Unmarshal(payload[:recLen], record)
	if err != nil {
		return nil, err
	}

	switch record.Type {
	case protos.LogRecord_ENTRY, protos.LogRecord_CONTROL:
		if !verifyCRC(r.crc, crc, payload) {
			return nil, errors.New("crc verification failed")
		}
		fallthrough
	case protos.LogRecord_CRC_ANCHOR:
		r.crc = crc
	default:
		return nil, fmt.Errorf("unexpected LogRecord_Type: %v", record.Type)
	}

	return record, nil
}

func (r *LogRecordReader) readHeader() (length, crc uint32, err error) {
	buff := make([]byte, 8)
	n, err := r.logFile.Read(buff)
	if err != nil {
		return 0, 0, err
	}
	if n != 8 {
		return 0, 0, fmt.Errorf("incomplete header: size: expected=%d, actual=%d", 8, n)
	}

	header := binary.LittleEndian.Uint64(buff)
	length = uint32(header & recordLengthMask)
	crc = uint32((header & recordCRCMask) >> 32)

	return length, crc, nil
}

func (r *LogRecordReader) readPayload(len int) (payload []byte, err error) {
	buff := make([]byte, len)
	n, err := r.logFile.Read(buff)
	if err != nil {
		return nil, err
	}
	if n != len {
		return nil, fmt.Errorf("incomplete payload: size: expected=%d, actual=%d", len, n)
	}
	return buff, nil
}

func verifyCRC(prevCRC, expectedCRC uint32, data []byte) bool {
	dataCRC := crc32.Update(prevCRC, crcTable, data)
	return dataCRC == expectedCRC
}

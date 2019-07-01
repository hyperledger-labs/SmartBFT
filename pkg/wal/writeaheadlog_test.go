// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"fmt"
	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteAheadLogFile_Create(t *testing.T) {
	testDir, err := ioutil.TempDir("", "unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	t.Run("Good", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good")

		wal, err := Create(logger, dirPath, nil)
		assert.NoError(t, err)
		assert.NotNil(t, wal)

		var crc uint32
		if wal != nil {
			crc = wal.CRC()
			err = wal.Close()
			assert.NoError(t, err)
		}

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})

	t.Run("Good - with options", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good-w-options")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		assert.NoError(t, err)

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 100 * 1024, BufferSizeBytes: 1024})
		assert.NoError(t, err)
		assert.NotNil(t, wal)

		var crc uint32
		if wal != nil {
			err = wal.Close()
			assert.NoError(t, err)
			crc = wal.CRC()
		}

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})

	t.Run("Bad - already exist", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "bad-exist")
		err := os.MkdirAll(dirPath, walDirPermPrivateRWX)
		assert.NoError(t, err)
		f, err := os.Create(filepath.Join(dirPath, "0000000000000008.wal"))
		assert.NoError(t, err)
		assert.NotNil(t, f)
		err = f.Close()
		assert.NoError(t, err)

		wal, err := Create(logger, dirPath, nil)
		assert.Error(t, err)
		if err != nil {
			assert.True(t, strings.HasPrefix(err.Error(), "wal: directory not empty:"))
		}
		assert.Nil(t, wal)
	})
}

func TestWriteAheadLogFile_Close(t *testing.T) {
	testDir, err := ioutil.TempDir("", "unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	t.Run("Idempotent", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "idempotent")

		wal, err := Create(logger, dirPath, nil)
		assert.NoError(t, err)
		assert.NotNil(t, wal)

		if wal == nil {
			return
		}

		crc := wal.CRC()
		err = wal.Close()
		assert.NoError(t, err)
		err = wal.Close()
		assert.NoError(t, err)
		assert.Equal(t, crc, wal.CRC())

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})

	t.Run("Cannot Append", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "cannot-append")

		wal, err := Create(logger, dirPath, nil)
		assert.NoError(t, err)
		assert.NotNil(t, wal)

		if wal == nil {
			return
		}

		crc := wal.CRC()
		err = wal.Close()
		assert.NoError(t, err)
		err = wal.Append([]byte{1,2,3,4}, true)
		assert.EqualError(t, err, os.ErrClosed.Error())

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyFirstFileCreation(t, logger, dirPath, expectedFileName, crc)
	})
}

func TestWriteAheadLogFile_Append(t *testing.T) {
	testDir, err := ioutil.TempDir("", "unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	t.Run("Good", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "good")

		wal, err := Create(logger, dirPath, nil)
		assert.NoError(t, err)
		assert.NotNil(t, wal)
		if wal == nil {
			return
		}

		rec1 := &smartbftprotos.LogRecord{
			Type: smartbftprotos.LogRecord_ENTRY,
			TruncateTo: true,
			Data: []byte{1, 2, 3, 4},
		}
		err = wal.Append(rec1.Data, rec1.TruncateTo)
		assert.NoError(t, err)

		rec2 := &smartbftprotos.LogRecord{
			Type: smartbftprotos.LogRecord_ENTRY,
			TruncateTo: false,
			Data: []byte{5, 6, 7, 8, 9, 10, 11, 12},
		}
		err = wal.Append(rec2.Data, rec2.TruncateTo)
		assert.NoError(t, err)

		err = wal.Append(nil, false)
		assert.Error(t, err)

		err = wal.Append([]byte{}, false)
		assert.Error(t, err)

		crc := wal.CRC()
		err = wal.Close()
		assert.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyAppend(t, logger, dirPath, expectedFileName, crc, rec1, rec2)
	})

	t.Run("File switch", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "switch")

		wal, err := Create(logger, dirPath, &Options{FileSizeBytes: 10 * 1024, BufferSizeBytes: 2048})
		assert.NoError(t, err)
		assert.NotNil(t, wal)
		if wal == nil {
			return
		}

		N := 1024
		M := 20

		records := make([]*smartbftprotos.LogRecord, M)
		var crc1, crc2 uint32
		for m := 0; m < M; m++ {

			data1 := make([]byte, N)
			for n := 0; n < N; n++ {
				data1[n] = byte(n % (m + 1))
			}

			rec := &smartbftprotos.LogRecord{
				Type: smartbftprotos.LogRecord_ENTRY,
				TruncateTo: false,
				Data: data1,
			}
			if m==0 {
				rec.TruncateTo = true
			}

			records[m] = rec

			err = wal.Append(rec.Data, rec.TruncateTo)
			assert.NoError(t, err)

			if m==9 {
				crc1 = wal.CRC()
			}
		}
		crc2 = wal.CRC()

		err = wal.Close()
		assert.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyAppend(t, logger, dirPath, expectedFileName, crc1, records[:10]...)
		expectedFileName = fmt.Sprintf(walFileTemplate, 2)
		verifyAppend(t, logger, dirPath, expectedFileName, crc2, records[10:]...)
	})


	t.Run("TruncateTo", func(t *testing.T) {
		dirPath := filepath.Join(testDir, "TruncateTo")

		wal, err := Create(logger, dirPath, nil)
		assert.NoError(t, err)
		assert.NotNil(t, wal)
		if wal == nil {
			return
		}

		rec1 := &smartbftprotos.LogRecord{
			Type: smartbftprotos.LogRecord_ENTRY,
			TruncateTo: false,
			Data: []byte{1, 2, 3, 4},
		}
		err = wal.Append(rec1.Data, rec1.TruncateTo)
		assert.NoError(t, err)

		rec2 := &smartbftprotos.LogRecord{
			Type: smartbftprotos.LogRecord_CONTROL,
			TruncateTo: true,
		}
		err = wal.TruncateTo()
		assert.NoError(t, err)

		rec3 := &smartbftprotos.LogRecord{
			Type: smartbftprotos.LogRecord_ENTRY,
			TruncateTo: false,
			Data: []byte{5, 6, 7, 8, 9, 10, 11, 12},
		}
		err = wal.Append(rec3.Data, rec3.TruncateTo)
		assert.NoError(t, err)

		err = wal.Append(nil, false)
		assert.Error(t, err)

		err = wal.Append([]byte{}, false)
		assert.Error(t, err)

		crc := wal.CRC()
		err = wal.Close()
		assert.NoError(t, err)

		expectedFileName := fmt.Sprintf(walFileTemplate, 1)
		verifyAppend(t, logger, dirPath, expectedFileName, crc, rec1, rec2, rec3)
	})

}

func verifyFirstFileCreation(t *testing.T, logger api.Logger, dirPath string, expectedFileName string, expectedCRC uint32) {
	names, err := dirReadWalNames(dirPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(names))
	assert.Equal(t, expectedFileName, names[0])

	r, err := NewLogRecordReader(logger, filepath.Join(dirPath, expectedFileName))
	assert.NoError(t, err)
	assert.NotNil(t, r)
	if r != nil {
		defer r.Close()
		record, err := r.Read()
		assert.Error(t, err, "no more records")
		assert.Nil(t, record)
		assert.Equal(t, expectedCRC, r.CRC())
	}
}

func verifyAppend(t *testing.T, logger api.Logger, dirPath string, expectedFileName string, expectedCRC uint32, records ...*smartbftprotos.LogRecord) {
	r, err := NewLogRecordReader(logger, filepath.Join(dirPath, expectedFileName))
	assert.NoError(t, err)
	assert.NotNil(t, r)
	if r == nil {
		return
	}
	defer r.Close()

	for _, expectedRecord := range records {
		record, err := r.Read()
		assert.NoError(t, err)
		assert.NotNil(t, record)
		assert.Equal(t, expectedRecord.Data, record.Data)
		assert.Equal(t, expectedRecord.Type, record.Type)
		assert.Equal(t, expectedRecord.TruncateTo, record.TruncateTo)
	}

	record, err := r.Read()
	assert.Error(t, err, "no more records")
	assert.Nil(t, record)
	assert.Equal(t, expectedCRC, r.CRC())
}

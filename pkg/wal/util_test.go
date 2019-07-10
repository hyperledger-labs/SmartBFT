// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestWALUtil(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	t.Run("read wal names", func(t *testing.T) {
		testDir, err := ioutil.TempDir("", "unittest")
		assert.NoErrorf(t, err, "generate temporary test dir")
		defer os.RemoveAll(testDir)

		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(names))

		make8LogFiles(t, logger, testDir)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		nameSet := arrayToSet(names)
		assert.Equal(t, 8, len(nameSet))
		for i := 1; i <= 8; i++ {
			fn := fmt.Sprintf(walFileTemplate, i)
			assert.True(t, nameSet[fn])
		}

		fnOld := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1))
		fnNew := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 1)+".copy")
		err = os.Rename(fnOld, fnNew)
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		nameSet = arrayToSet(names)
		assert.Equal(t, 7, len(nameSet))
		for i := 2; i <= 8; i++ {
			fn := fmt.Sprintf(walFileTemplate, i)
			assert.True(t, nameSet[fn])
		}

		fnOld = filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 8))
		fnNew = filepath.Join(testDir, "oops-"+fmt.Sprintf(walFileTemplate, 8))
		err = os.Rename(fnOld, fnNew)
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		nameSet = arrayToSet(names)
		assert.Equal(t, 6, len(nameSet))
		for i := 2; i <= 7; i++ {
			fn := fmt.Sprintf(walFileTemplate, i)
			assert.True(t, nameSet[fn])
		}

		names, err = dirReadWalNames(testDir + ".does-not-exist")
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, names)
	})

	t.Run("check wal names", func(t *testing.T) {
		testDir, err := ioutil.TempDir("", "unittest")
		assert.NoErrorf(t, err, "generate temporary test dir")
		defer os.RemoveAll(testDir)

		indexes, err := checkWalFiles(logger, testDir, []string{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(indexes))

		make8LogFiles(t, logger, testDir)

		//All good
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		indexes, err = checkWalFiles(logger, testDir, names)
		assert.Equal(t, 8, len(indexes))
		for i := 1; i <= 8; i++ {
			assert.Equal(t, uint64(i), indexes[i-1])
		}

		//Dir does not exist
		indexes, err = checkWalFiles(logger, testDir+".does-not-exist", names)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, indexes)

		//Gap in sequence
		fn4 := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 4))
		err = os.Remove(fn4)
		assert.NoError(t, err)
		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		indexes, err = checkWalFiles(logger, testDir, names)
		assert.EqualError(t, err, "wal: files not in sequence")

		//File does not exist
		names = append(names, fmt.Sprintf(walFileTemplate, 4))
		indexes, err = checkWalFiles(logger, testDir, names)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Contains(t, err.Error(), "wal: failed to create reader for file:")

		//Error in the last file
		for i := 1; i <= 3; i++ {
			fn := filepath.Join(testDir, fmt.Sprintf(walFileTemplate, i))
			err = os.Remove(fn)
			assert.NoError(t, err)
		}
		f, err := os.Create(filepath.Join(testDir, fmt.Sprintf(walFileTemplate, 9))) //No anchor
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		indexes, err = checkWalFiles(logger, testDir, names)
		assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())
	})

	t.Run("rename-reset", func(t *testing.T) {
		testDir, err := ioutil.TempDir("", "unittest")
		assert.NoErrorf(t, err, "generate temporary test dir")
		defer os.RemoveAll(testDir)

		make8LogFiles(t, logger, testDir)
		names, err := dirReadWalNames(testDir)
		assert.NoError(t, err)
		indexes, err := checkWalFiles(logger, testDir, names)
		assert.Equal(t, 8, len(indexes))
		nextFileName := fmt.Sprintf(walFileTemplate, indexes[len(indexes)-1]+1)
		err = renameResetWalFile(filepath.Join(testDir, names[0]), filepath.Join(testDir, nextFileName))
		assert.NoError(t, err)

		names, err = dirReadWalNames(testDir)
		assert.NoError(t, err)
		indexes, err = checkWalFiles(logger, testDir, names[:len(names)-1])
		assert.NoError(t, err)
		for i := 0; i <= 6; i++ {
			assert.Equal(t, uint64(i+2), indexes[i])
		}

		//Last file has no CRC-Anchor
		indexes, err = checkWalFiles(logger, testDir, names)
		assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())
	})
}

func arrayToSet(array []string) map[string]bool {
	set := make(map[string]bool, 0)
	for _, n := range array {
		set[n] = true
	}
	return set
}

// create 8 wal files, 000000000000000000001.wal - 000000000000000000008.wal
func make8LogFiles(t *testing.T, logger api.Logger, testDir string) {
	wal, err := Create(logger, testDir, &Options{FileSizeBytes: 2048})
	assert.NoError(t, err)
	assert.NotNil(t, wal)
	if wal == nil {
		return
	}

	rec1 := &smartbftprotos.LogRecord{
		Type:       smartbftprotos.LogRecord_ENTRY,
		TruncateTo: false,
		Data:       make([]byte, 512),
	}

	for i := 0; i < 30; i++ {
		err = wal.Append(rec1.Data, rec1.TruncateTo)
		assert.NoError(t, err)
	}
	_ = wal.Close()
}

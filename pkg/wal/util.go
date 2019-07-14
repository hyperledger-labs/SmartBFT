// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/SmartBFT-Go/consensus/pkg/api"
)

var padTable [][]byte

func init() {
	padTable = make([][]byte, 8)
	for i := 0; i < 8; i++ {
		padTable[i] = make([]byte, i)
	}
}

func dirEmpty(dirPath string) bool {
	names, err := dirReadWalNames(dirPath)
	if err != nil {
		return true
	}

	return len(names) == 0
}

func dirCreate(dirPath string) error {
	dirFile, err := os.Open(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirPath, walDirPermPrivateRWX)
		}
		return err
	}
	defer dirFile.Close()
	return err
}

// dirReadWalNames finds file names that follow the wal file name template.
func dirReadWalNames(dirPath string) ([]string, error) {
	dirFile, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer dirFile.Close()

	names, err := dirFile.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	walNames := make([]string, 0)
	for _, name := range names {
		if strings.HasSuffix(name, walFileSuffix) {
			var index uint64
			n, err := fmt.Sscanf(name, walFileTemplate, &index)
			if n != 1 || err != nil {
				continue
			}
			walNames = append(walNames, name)
		}
	}

	sort.Strings(walNames)

	return walNames, nil
}

// checkWalFiles for continuous sequence, readable CRC-Anchor.
// If the the last file cannot be read, it may be ignored,  (or repaired).
func checkWalFiles(logger api.Logger, dirName string, walNames []string) ([]uint64, error) {
	sort.Strings(walNames)
	var indexes = make([]uint64, 0)
	for i, name := range walNames {
		index, err := parseWalFileName(name)
		if err != nil {
			logger.Errorf("wal: failed to parse file name: %s; error: %s", name, err)
			return nil, err
		}
		indexes = append(indexes, index)

		// verify we have CRC-Anchor.
		r, err := NewLogRecordReader(logger, filepath.Join(dirName, walNames[i]))
		if err != nil {
			// check if it is the last file and return a special error that allows a repair.
			if i == len(walNames)-1 {
				logger.Errorf("wal: failed to create reader for last file: %s; error: %s; this may possibly be repaired.", name, err)
				return nil, io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("wal: failed to create reader for file: %s; error: %s", name, err)
		}
		err = r.Close()
		if err != nil {
			return nil, fmt.Errorf("wal: failed to close reader for file: %s; error: %s", name, err)
		}

		//verify no gaps
		if i == 0 {
			continue
		}
		if index != (indexes[i-1] + 1) {
			return nil, errors.New("wal: files not in sequence")
		}
	}

	sort.Slice(indexes,
		func(i, j int) bool {
			return indexes[i] < indexes[j]
		},
	)

	return indexes, nil
}

func getPadSize(recordLength int) int {
	return (8 - recordLength%8) % 8
}

func getPadBytes(recordLength int) (int, []byte) {
	i := getPadSize(recordLength)
	return i, padTable[i]
}

func parseWalFileName(fileName string) (index uint64, err error) {
	n, err := fmt.Sscanf(fileName, walFileTemplate, &index)
	if n != 1 || err != nil {
		return 0, fmt.Errorf("failed to parse wal file name: %s; error: %s", fileName, err)
	}
	return index, nil
}

// renameResetWalFile reset anchor on a temporary file, and then rename to next file name.
func renameResetWalFile(recycleFile, nextFile string) (err error) {
	tmpFilePath := recycleFile + ".tmp"
	if err = os.Rename(recycleFile, tmpFilePath); err != nil {
		return err
	}
	tmpF, err := os.OpenFile(tmpFilePath, os.O_RDWR, walFilePermPrivateRW)
	if err != nil {
		return err
	}
	if _, err = tmpF.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = tmpF.Write(make([]byte, 1024)); err != nil { // overwrite the CRC-Anchor
		return err
	}
	if err = tmpF.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpFilePath, nextFile); err != nil {
		return err
	}

	return nil
}

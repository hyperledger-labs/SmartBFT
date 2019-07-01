// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"os"
	"sort"
	"strings"
)


var padTable  [][]byte

func init() {
	padTable = make([][]byte,8)
	for i := 0; i<8; i++ {
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
			walNames = append(walNames, name)
		}
	}
	sort.Strings(walNames)

	return walNames, nil
}

func getPadSize(recordLength int) int {
	return (8 - recordLength%8) % 8
}

func getPadBytes(recordLength int) (int, []byte) {
	i := getPadSize(recordLength)
	return i, padTable[i]
}





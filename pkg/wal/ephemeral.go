// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import "errors"

type ephemeralWALRecord struct {
	data       []byte
	truncateTo bool
}

type EphemeralWAL struct {
	records []*ephemeralWALRecord
}

func (ew *EphemeralWAL) Append(data []byte, truncateTo bool) error {
	if len(data) == 0 {
		return errors.New("data is nil or empty")
	}

	if truncateTo {
		ew.records = ew.records[0:0]
	}
	ew.records = append(ew.records, &ephemeralWALRecord{data: data, truncateTo: truncateTo})

	return nil
}

func (ew *EphemeralWAL) ReadAll() ([][]byte, error) {
	var clone = make([][]byte, 0)

	for _, entry := range ew.records {
		entryClone := make([]byte, len(entry.data))
		copy(entryClone, entry.data)
		clone = append(clone, entryClone)
	}

	return clone, nil
}

func (ew *EphemeralWAL) TruncateTo() error {
	ew.records = ew.records[0:0]
	return nil
}

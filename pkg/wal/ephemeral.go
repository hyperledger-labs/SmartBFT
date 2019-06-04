// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

type EphemeralWAL struct {
	data [][]byte
}

func (ew *EphemeralWAL) Append(entry []byte) {
	ew.data = append(ew.data, entry)
}

func (ew *EphemeralWAL) Read() [][]byte {
	var clone [][]byte
	for _, entry := range ew.data {
		entryClone := make([]byte, len(entry))
		copy(entryClone, entry)
		clone = append(clone, entryClone)
	}
	return clone
}

func (ew *EphemeralWAL) Truncate() {
	ew.data = nil
}

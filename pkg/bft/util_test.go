// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeDigest(t *testing.T) {
	assert.Equal(t, "74f81fe167d99b4cb41d6d0ccda82278caee9f3e2f25d5e5a3936ff3dcec60d0", ComputeDigest([]byte{1, 2, 3, 4, 5}))
}

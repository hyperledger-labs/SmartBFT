// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package network

import (
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/test"
	"github.com/stretchr/testify/assert"
)

func TestNetwork(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		nodes := []uint64{1, 2, 3, 4}
		m := NewMesh(10*time.Millisecond, nodes...)
		assert.Equal(t, nodes, m.NodeIDs())
	})

	t.Run("get queue", func(t *testing.T) {
		nodes := []uint64{1, 2, 3, 4}
		m := NewMesh(10*time.Millisecond, nodes...)
		for _, n := range nodes {
			inQ := m.getInQ(n)
			assert.NotNil(t, inQ)
		}

		inQ := m.getInQ(5)
		assert.Nil(t, inQ)
	})

	t.Run("send", func(t *testing.T) {
		nodes := []uint64{1, 2, 3, 4}
		m := NewMesh(100*time.Millisecond, nodes...)
		err := m.send(1, 2, &test.FwdMessage{Sender: 1, Payload: []byte{6, 6, 6}})
		assert.NoError(t, err)
		inQ2 := m.getInQ(2)
		msg := <-inQ2
		assert.Equal(t, uint64(1), msg.from)
		assert.Equal(t, uint64(1), msg.message.(*test.FwdMessage).Sender)
		assert.Equal(t, []byte{6, 6, 6}, msg.message.(*test.FwdMessage).Payload)
	})
}

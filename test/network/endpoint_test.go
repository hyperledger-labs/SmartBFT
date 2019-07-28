// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package network

import (
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/test"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestEndpoint(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		nodes := []uint64{1, 2, 3, 4}
		m := NewMesh(10*time.Millisecond, nodes...)
		r := func(from uint64, msg proto.Message) {}
		ep := NewEndpoint(m, 1, r)
		assert.NotNil(t, ep)
		ep = NewEndpoint(m, 5, r)
		assert.Nil(t, ep)
	})

	t.Run("start stop", func(t *testing.T) {
		nodes := []uint64{1, 2, 3, 4}
		m := NewMesh(10*time.Millisecond, nodes...)
		r := func(from uint64, msg proto.Message) {}
		ep := NewEndpoint(m, 1, r)
		assert.NotNil(t, ep)
		ep.Start()
		ep.Stop()
		ep.Stop()
	})

	t.Run("send", func(t *testing.T) {
		nodes := []uint64{1, 2}
		m := NewMesh(100*time.Millisecond, nodes...)

		c1 := 0
		r1 := func(from uint64, msg proto.Message) {
			assert.Equal(t, uint64(2), from)
			assert.Equal(t, uint64(2), msg.(*test.FwdMessage).Sender)
			assert.Equal(t, []byte{2, 2, 2}, msg.(*test.FwdMessage).Payload)
			c1++
		}
		ep1 := NewEndpoint(m, 1, r1)
		assert.NotNil(t, ep1)
		ep1.Start()

		c2 := 0
		r2 := func(from uint64, msg proto.Message) {
			assert.Equal(t, uint64(1), from)
			assert.Equal(t, uint64(1), msg.(*test.FwdMessage).Sender)
			assert.Equal(t, []byte{1, 1, 1}, msg.(*test.FwdMessage).Payload)
			c2++
		}
		ep2 := NewEndpoint(m, 2, r2)
		assert.NotNil(t, ep2)
		ep2.Start()

		err := ep1.Send(2, &test.FwdMessage{Sender: 1, Payload: []byte{1, 1, 1}})
		assert.NoError(t, err)

		err = ep2.Send(1, &test.FwdMessage{Sender: 2, Payload: []byte{2, 2, 2}})
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		ep1.Stop()
		ep2.Stop()

		assert.Equal(t, 1, c1)
		assert.Equal(t, 1, c2)
	})
}

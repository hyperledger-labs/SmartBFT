// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type membership struct {
}

func (m *membership) MembershipChange() bool {
	return false
}

var viewChangeMsg = &protos.Message{
	Content: &protos.Message_ViewChange{
		ViewChange: &protos.ViewChange{
			NextView: 1,
			Reason:   "",
		},
	},
}

var heartBeatMsg = &protos.Message{
	Content: &protos.Message_HeartBeat{
		HeartBeat: &protos.HeartBeat{
			View: 1,
			Seq:  1,
		},
	},
}

func TestMessagesDecoding(t *testing.T) {
	RegisterDecoders()
	RegisterSanitizers()
	recording := &bytes.Buffer{}
	proxy := &Proxy{Out: recording, MembershipNotifier: &membership{}}

	proxy.MembershipChange()
	proxy.MaybeRecordMessage(1, viewChangeMsg)
	proxy.MaybeRecordMessage(2, viewChangeMsg)
	proxy.MaybeRecordMessage(3, viewChangeMsg)
	proxy.MaybeRecordMessage(1, heartBeatMsg)
	proxy.MembershipChange()
	proxy.MaybeRecordMessage(4, viewChangeMsg)
	proxy.MaybeRecordMessage(1, heartBeatMsg)
	proxy.MaybeRecordMessage(3, viewChangeMsg)
	proxy.MembershipChange()
	proxy.MaybeRecordMessage(3, heartBeatMsg)
	proxy.MaybeRecordMessage(2, viewChangeMsg)
	proxy.MaybeRecordMessage(1, viewChangeMsg)

	proxy.Out = nil
	proxy.In = recording
	handleWG := sync.WaitGroup{}
	proxy.HandleMessage = func(sender uint64, m *protos.Message) { handleWG.Done() }
	proxy.StartDecoding()
	assert.True(t, strings.HasPrefix(proxy.next.String(), "Member"))

	handleWG.Add(4)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()

	handleWG.Add(3)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()

	handleWG.Add(3)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()
}

func TestMessagesDecodingWithDifferentOrder(t *testing.T) {
	RegisterDecoders()
	RegisterSanitizers()
	recording := &bytes.Buffer{}
	proxy := &Proxy{Out: recording, MembershipNotifier: &membership{}}

	proxy.MaybeRecordMessage(1, heartBeatMsg)
	proxy.MaybeRecordMessage(1, viewChangeMsg)
	proxy.MaybeRecordMessage(2, viewChangeMsg)
	proxy.MaybeRecordMessage(3, viewChangeMsg)
	proxy.MembershipChange()
	proxy.MembershipChange()
	proxy.MaybeRecordMessage(4, viewChangeMsg)
	proxy.MaybeRecordMessage(3, viewChangeMsg)
	proxy.MembershipChange()

	proxy.Out = nil
	proxy.In = recording
	handleWG := sync.WaitGroup{}
	proxy.HandleMessage = func(sender uint64, m *protos.Message) { handleWG.Done() }

	handleWG.Add(4)
	proxy.StartDecoding()
	handleWG.Wait()
	assert.True(t, strings.HasPrefix(proxy.next.String(), "Member"))

	assert.False(t, proxy.MembershipChange())

	handleWG.Add(2)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()

	assert.False(t, proxy.eof)

	assert.False(t, proxy.MembershipChange())

	assert.True(t, proxy.eof)
}

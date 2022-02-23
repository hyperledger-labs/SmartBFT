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

func TestMessagesDecoding(t *testing.T) {
	RegisterDecoders(nil)
	RegisterSanitizers()
	recording := &bytes.Buffer{}
	proxy := &Proxy{Out: recording, MembershipNotifier: &membership{}}

	proxy.MembershipChange()
	proxy.MaybeRecordMessage(1, viewChangeMsg)
	proxy.MaybeRecordMessage(2, viewChangeMsg)
	proxy.MaybeRecordMessage(3, viewChangeMsg)
	proxy.MembershipChange()
	proxy.MaybeRecordMessage(4, viewChangeMsg)
	proxy.MaybeRecordMessage(3, viewChangeMsg)
	proxy.MembershipChange()
	proxy.MaybeRecordMessage(2, viewChangeMsg)
	proxy.MaybeRecordMessage(1, viewChangeMsg)

	proxy.Out = nil
	proxy.In = recording
	handleWG := sync.WaitGroup{}
	proxy.HandleMessage = func(sender uint64, m *protos.Message) { handleWG.Done() }
	proxy.StartDecoding()
	assert.True(t, strings.HasPrefix(proxy.next.String(), "Member"))

	handleWG.Add(3)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()

	handleWG.Add(2)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()

	handleWG.Add(2)
	assert.False(t, proxy.MembershipChange())
	handleWG.Wait()
}

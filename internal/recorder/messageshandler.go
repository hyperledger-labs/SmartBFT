// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"strings"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type MessagesHandler struct {
	handler  func(sender uint64, m *smartbftprotos.Message)
	stopChan chan struct{}
	messages chan []types.RecordedEvent
	done     func()
}

func newMessagesHandler(h func(sender uint64, m *smartbftprotos.Message), done func()) *MessagesHandler {
	mh := &MessagesHandler{
		handler:  h,
		done:     done,
		stopChan: make(chan struct{}),
		messages: make(chan []types.RecordedEvent),
	}
	go func() {
		mh.run()
	}()
	return mh
}

func (mh *MessagesHandler) stop() {
	close(mh.stopChan)
}

func (mh *MessagesHandler) run() {
	for {
		select {
		case <-mh.stopChan:
			return
		case messages := <-mh.messages:
			mh.handleMessages(messages)
		}
	}
}

func (mh *MessagesHandler) handleMessages(messages []types.RecordedEvent) {
	for _, re := range messages {
		if !strings.HasPrefix(re.String(), "Message") {
			panic("recorded event is not a message")
		}
		decoded := decodeMessage(re.Content).(RecordedMessage)
		mh.handler(decoded.Sender, decoded.M)
	}
	mh.done()
}

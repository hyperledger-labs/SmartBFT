// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"strings"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type MessagesHandler struct {
	handler  func(sender uint64, m *protos.Message)
	stopChan chan struct{}
	messages chan []types.RecordedEvent
}

func newMessagesHandler(h func(sender uint64, m *protos.Message)) *MessagesHandler {
	mh := &MessagesHandler{
		handler:  h,
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

func (mh *MessagesHandler) getMessages(messages []types.RecordedEvent) {
	select {
	case <-mh.stopChan:
		return
	case mh.messages <- messages:
	}
}

func (mh *MessagesHandler) run() {
	for {
		select {
		case <-mh.stopChan:
			return
		case messages := <-mh.messages:
			time.Sleep(1 * time.Second)
			mh.handleMessages(messages)
		}
	}
}

func (mh *MessagesHandler) handleMessages(messages []types.RecordedEvent) {
	for _, re := range messages {
		if !strings.HasPrefix(re.String(), "Message") {
			panic("recorded event is not a message")
		}
		decoded := decodeMessages(re.Content).(RecordedMessages)
		m := &protos.Message{}
		switch re.Type {
		case TypeMessagePrePrepare:
			m = &protos.Message{Content: &protos.Message_PrePrepare{PrePrepare: decoded.PREP}}
		case TypeMessagePrepare:
			m = &protos.Message{Content: &protos.Message_Prepare{Prepare: decoded.P}}
		case TypeMessageCommit:
			m = &protos.Message{Content: &protos.Message_Commit{Commit: decoded.CMT}}
		case TypeMessageViewChange:
			m = &protos.Message{Content: &protos.Message_ViewChange{ViewChange: decoded.VC}}
		case TypeMessageViewData:
			m = &protos.Message{Content: &protos.Message_ViewData{ViewData: decoded.VD}}
		case TypeMessageNewView:
			m = &protos.Message{Content: &protos.Message_NewView{NewView: decoded.NV}}
		case TypeMessageHeartBeat:
			m = &protos.Message{Content: &protos.Message_HeartBeat{HeartBeat: decoded.HB}}
		case TypeMessageHeartBeatResponse:
			m = &protos.Message{Content: &protos.Message_HeartBeatResponse{HeartBeatResponse: decoded.HBR}}
		case TypeMessageStateTransferRequest:
			m = &protos.Message{Content: &protos.Message_StateTransferRequest{StateTransferRequest: decoded.STRequest}}
		case TypeMessageStateTransferResponse:
			m = &protos.Message{Content: &protos.Message_StateTransferResponse{StateTransferResponse: decoded.STResponse}}
		}
		mh.handler(decoded.Sender, m)
	}
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

const (
	TypeSyncResponse                 types.EventType = "SyncResponse"
	TypeDecisionAndResponse          types.EventType = "DecisionAndResponse"
	TypeSignResponse                 types.EventType = "SignResponse"
	TypeSignedProposal               types.EventType = "SignedProposal"
	TypeProposal                     types.EventType = "Proposal"
	TypeMembershipChange             types.EventType = "MembershipChange"
	TypeMessageStateTransferRequest  types.EventType = "MessageStateTransferRequest"
	TypeMessageStateTransferResponse types.EventType = "MessageStateTransferResponse"
	TypeMessageHeartBeat             types.EventType = "MessageHeartBeat"
	TypeMessageHeartBeatResponse     types.EventType = "MessageHeartBeatResponse"
	TypeMessagePrePrepare            types.EventType = "MessagePrePrepare"
	TypeMessagePrepare               types.EventType = "MessagePrepare"
	TypeMessageCommit                types.EventType = "MessageCommit"
	TypeMessageViewChange            types.EventType = "MessageViewChange"
	TypeMessageViewData              types.EventType = "MessageViewData"
	TypeMessageNewView               types.EventType = "MessageNewView"
)

func RegisterSanitizers() {
	types.RegisterSanitizer(TypeSyncResponse, sanitizeSync)
	types.RegisterSanitizer(TypeDecisionAndResponse, sanitizeDecision)
	types.RegisterSanitizer(TypeSignResponse, sanitizeToNil)
	types.RegisterSanitizer(TypeSignedProposal, sanitizeSignedProposal)
	types.RegisterSanitizer(TypeProposal, sanitizeProposal)
	types.RegisterSanitizer(TypeMembershipChange, nothingToSanitize)
	types.RegisterSanitizer(TypeMessageStateTransferRequest, nothingToSanitize)
	types.RegisterSanitizer(TypeMessageStateTransferResponse, nothingToSanitize)
	types.RegisterSanitizer(TypeMessageHeartBeat, nothingToSanitize)
	types.RegisterSanitizer(TypeMessageHeartBeatResponse, nothingToSanitize)
	types.RegisterSanitizer(TypeMessagePrePrepare, sanitizePrePrepare)
	types.RegisterSanitizer(TypeMessagePrepare, nothingToSanitize)
	types.RegisterSanitizer(TypeMessageCommit, sanitizeCommit)
	types.RegisterSanitizer(TypeMessageViewChange, nothingToSanitize)
	types.RegisterSanitizer(TypeMessageViewData, sanitizeViewData)
	types.RegisterSanitizer(TypeMessageNewView, sanitizeNewView)
}

func RegisterDecoders(wrapper func(func([]byte) interface{}) func([]byte) interface{}) {
	if wrapper == nil {
		wrapper = func(f func([]byte) interface{}) func([]byte) interface{} {
			return f
		}
	}
	types.RegisterDecoder(TypeSyncResponse, wrapper(decodeSanitizedResponse))
	types.RegisterDecoder(TypeDecisionAndResponse, wrapper(decodeSanitizedDecision))
	types.RegisterDecoder(TypeSignResponse, wrapper(decodeFromNil))
	types.RegisterDecoder(TypeSignedProposal, wrapper(decodeSanitizedSignedProposal))
	types.RegisterDecoder(TypeProposal, wrapper(decodeSanitizedProposal))
	types.RegisterDecoder(TypeMembershipChange, wrapper(decodeBool))
}

type Proxy struct {
	Logger             api.Logger
	once               sync.Once
	in                 *bufio.Scanner
	Synchronizer       api.Synchronizer
	Application        api.Application
	Signer             api.Signer
	Assembler          api.Assembler
	MembershipNotifier api.MembershipNotifier
	Out                io.Writer
	In                 io.Reader
	lock               sync.Mutex
	mh                 *MessagesHandler
	HandleMessage      func(sender uint64, m *protos.Message)
	next               types.RecordedEvent
	eof                bool
}

func (p *Proxy) getOrCreateInput() *bufio.Scanner {
	p.once.Do(func() {
		p.in = bufio.NewScanner(p.In)
		p.mh = newMessagesHandler(p.HandleMessage)
	})
	return p.in
}

func (p *Proxy) nextRecord() interface{} {
	if p.eof {
		panic("reached end of file")
	}
	in := p.getOrCreateInput()
	next := p.next
	var messages []types.RecordedEvent
	for {
		if !in.Scan() {
			p.eof = true
			break
		}
		re := types.RecordedEvent{}
		re.FromString(in.Text())
		if !strings.HasPrefix(re.String(), "Message") {
			p.next = re
			break
		}
		messages = append(messages, re)
	}
	if len(messages) > 0 {
		p.mh.getMessages(messages)
	}
	if p.eof {
		p.mh.stop()
	}
	return next.Decode()
}

func (p *Proxy) StartDecoding() {
	if p.eof {
		panic("reached end of file")
	}
	in := p.getOrCreateInput()
	var messages []types.RecordedEvent
	for {
		if !in.Scan() {
			p.eof = true
			break
		}
		re := types.RecordedEvent{}
		re.FromString(in.Text())
		if !strings.HasPrefix(re.String(), "Message") {
			p.next = re
			break
		}
		messages = append(messages, re)
	}
	if len(messages) > 0 {
		p.mh.getMessages(messages)
	}
	if p.eof {
		p.mh.stop()
	}
}

func (p *Proxy) write(re types.RecordedEvent) {
	p.lock.Lock()
	defer p.lock.Unlock()
	fmt.Fprintln(p.Out, re)
}

func (p *Proxy) Sync() types.SyncResponse {
	if p.Out != nil {
		res := p.Synchronizer.Sync()
		re := types.NewRecordedEvent(TypeSyncResponse, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		res := p.nextRecord().(types.SyncResponse)
		return res
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) Deliver(proposal types.Proposal, signature []types.Signature) types.Reconfig {
	if p.Out != nil {
		res := p.Application.Deliver(proposal, signature)
		re := types.NewRecordedEvent(TypeDecisionAndResponse, DecisionAndResponse{
			Reconfig: res,
			Decision: types.Decision{Proposal: proposal, Signatures: signature},
		})
		p.write(re)
		return res
	}

	if p.In != nil {
		res := p.nextRecord().(DecisionAndResponse)
		return res.Reconfig
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) Sign(b []byte) []byte {
	if p.Out != nil {
		res := p.Signer.Sign(b)
		re := types.NewRecordedEvent(TypeSignResponse, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		p.nextRecord()
		return nil
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) SignProposal(proposal types.Proposal, auxiliaryInput []byte) *types.Signature {
	if p.Out != nil {
		res := p.Signer.SignProposal(proposal, auxiliaryInput)
		re := types.NewRecordedEvent(TypeSignedProposal, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		return p.nextRecord().(*types.Signature)
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	if p.Out != nil {
		res := p.Assembler.AssembleProposal(metadata, requests)
		re := types.NewRecordedEvent(TypeProposal, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		return p.nextRecord().(types.Proposal)
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) MembershipChange() bool {
	if p.Out != nil {
		res := p.MembershipNotifier.MembershipChange()
		re := types.NewRecordedEvent(TypeMembershipChange, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		return p.nextRecord().(bool)
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) MaybeRecordMessage(sender uint64, m *protos.Message) {
	if p.Out != nil {
		var re types.RecordedEvent
		switch m.GetContent().(type) {
		case *protos.Message_PrePrepare:
			re = types.NewRecordedEvent(TypeMessagePrePrepare, RecordedMessages{Sender: sender, PREP: m.GetPrePrepare()})
		case *protos.Message_Prepare:
			re = types.NewRecordedEvent(TypeMessagePrepare, RecordedMessages{Sender: sender, P: m.GetPrepare()})
		case *protos.Message_Commit:
			re = types.NewRecordedEvent(TypeMessageCommit, RecordedMessages{Sender: sender, CMT: m.GetCommit()})
		case *protos.Message_ViewChange:
			re = types.NewRecordedEvent(TypeMessageViewChange, RecordedMessages{Sender: sender, VC: m.GetViewChange()})
		case *protos.Message_ViewData:
			re = types.NewRecordedEvent(TypeMessageViewData, RecordedMessages{Sender: sender, VD: m.GetViewData()})
		case *protos.Message_NewView:
			re = types.NewRecordedEvent(TypeMessageNewView, RecordedMessages{Sender: sender, NV: m.GetNewView()})
		case *protos.Message_HeartBeat:
			re = types.NewRecordedEvent(TypeMessageHeartBeat, RecordedMessages{Sender: sender, HB: m.GetHeartBeat()})
		case *protos.Message_HeartBeatResponse:
			re = types.NewRecordedEvent(TypeMessageHeartBeatResponse, RecordedMessages{Sender: sender, HBR: m.GetHeartBeatResponse()})
		case *protos.Message_StateTransferRequest:
			re = types.NewRecordedEvent(TypeMessageStateTransferRequest, RecordedMessages{Sender: sender, STRequest: m.GetStateTransferRequest()})
		case *protos.Message_StateTransferResponse:
			re = types.NewRecordedEvent(TypeMessageStateTransferResponse, RecordedMessages{Sender: sender, STResponse: m.GetStateTransferResponse()})
		default:
			p.Logger.Panicf("Unexpected message type")
		}
		p.write(re)
		return
	}

	if p.In != nil {
		return
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

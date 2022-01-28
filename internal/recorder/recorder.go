// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
)

const (
	TypeSyncResponse        types.EventType = "SyncResponse"
	TypeDecisionAndResponse types.EventType = "DecisionAndResponse"
)

func RegisterTypes() {
	types.RegisterDecoder(TypeSyncResponse, decodeSanitizedResponse)
	types.RegisterSanitizer(TypeSyncResponse, sanitizeSync)
	types.RegisterDecoder(TypeDecisionAndResponse, decodeSanitizedDecision)
	types.RegisterSanitizer(TypeDecisionAndResponse, sanitizeDecision)
}

type Proxy struct {
	Logger api.Logger
	once   sync.Once
	in     *bufio.Scanner
	S      api.Synchronizer
	A      api.Application
	Out    io.Writer
	In     io.Reader
}

func (p *Proxy) getOrCreateInput() *bufio.Scanner {
	p.once.Do(func() {
		p.in = bufio.NewScanner(p.In)
	})
	return p.in
}

func (p *Proxy) nextRecord() interface{} {
	in := p.getOrCreateInput()
	if !in.Scan() {
		panic("reached end of file")
	}
	re := types.RecordedEvent{}
	re.FromString(in.Text())
	return re.Decode()
}

func (p *Proxy) write(re types.RecordedEvent) {
	fmt.Fprintln(p.Out, re)
}

func (p *Proxy) Sync() types.SyncResponse {
	if p.Out != nil {
		res := p.S.Sync()
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
		res := p.A.Deliver(proposal, signature)
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

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

type StateRecorder struct {
	SavedMessages []*smartbftprotos.Message
}

func (sr *StateRecorder) Save(message *smartbftprotos.Message) error {
	sr.SavedMessages = append(sr.SavedMessages, message)
	return nil
}

func (*StateRecorder) Restore() (*View, error) {
	panic("should not be used")
}

type PersistedState struct {
	WAL api.WriteAheadLog
}

func (ps *PersistedState) Save(message *smartbftprotos.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		panic(fmt.Sprintf("failed marshaling message: %v", err))
	}
	// It is only safe to truncate if we either:
	//
	// 1) Process a pre-prepare, because it means we safely persisted the
	// previous proposal and have a stable checkpoint.
	// 2) Acquired a new view message which contains 2f+1 attestations
	//    of the cluster agreeing to a new view configuration.
	newProposal := message.GetPrePrepare() != nil
	finalizedView := message.GetNewView() != nil
	return ps.WAL.Append(b, newProposal || finalizedView)
}

func (ps *PersistedState) Restore() (*View, error) {
	panic("implement me")
}

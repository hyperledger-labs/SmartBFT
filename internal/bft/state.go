// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
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

func (*StateRecorder) Restore(_ *View) error {
	panic("should not be used")
}

type PersistedState struct {
	Logger api.Logger
	WAL    api.WriteAheadLog
}

func (ps *PersistedState) Save(message *smartbftprotos.Message) error {
	msgToSave := &smartbftprotos.SavedMessage{
		Msg: message,
	}
	b, err := proto.Marshal(msgToSave)
	if err != nil {
		ps.Logger.Panicf("Failed marshaling message: %v", err)
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

func (ps *PersistedState) Restore(v *View) error {
	// Unless we conclude otherwise, we're in a COMMITTED state
	v.Phase = COMMITTED

	entries, err := ps.WAL.ReadAll()
	if err != nil {
		ps.Logger.Errorf("Failed reading WAL: %v", err)
		return err
	}

	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil
	}

	ps.Logger.Infof("WAL contains %d entries", len(entries))

	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &smartbftprotos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return err
	}

	if lastPersistedMessage.Msg.GetPrePrepare() != nil {
		return recoverProposed(lastPersistedMessage.Msg, v, ps.Logger)
	}

	if commitMsg := lastPersistedMessage.Msg.GetCommit(); commitMsg != nil {
		return recoverPrepared(lastPersistedMessage.Msg, v, entries, ps.Logger)
	}

	// TODO: handle signed view data persisted in the WAL

	return nil
}

func recoverProposed(lastPersistedMessage *smartbftprotos.Message, v *View, logger api.Logger) error {
	prop := lastPersistedMessage.GetPrePrepare().Proposal
	v.inFlightProposal = &types.Proposal{
		VerificationSequence: int64(prop.VerificationSequence),
		Metadata:             prop.Metadata,
		Payload:              prop.Payload,
		Header:               prop.Header,
	}
	// Reconstruct the prepare message we shall next broadcast
	// after the recovery.
	v.lastBroadcastSent = &smartbftprotos.Message{
		Content: &smartbftprotos.Message_Prepare{
			Prepare: &smartbftprotos.Prepare{
				Seq:    lastPersistedMessage.GetPrePrepare().Seq,
				View:   lastPersistedMessage.GetPrePrepare().View,
				Digest: v.inFlightProposal.Digest(),
			},
		},
	}
	v.Phase = PROPOSED
	logger.Infof("Restored proposal with sequence %d", lastPersistedMessage.GetPrePrepare().Seq)
	return nil
}

func recoverPrepared(lastPersistedMessage *smartbftprotos.Message, v *View, entries [][]byte, logger api.Logger) error {
	// Last entry is a commit, so we should have not pruned the previous pre-prepare
	if len(entries) < 2 {
		return fmt.Errorf("last message is a commit, but expected to also have a matching pre-prepare")
	}
	prePrepareMsg := &smartbftprotos.SavedMessage{}
	if err := proto.Unmarshal(entries[len(entries)-2], prePrepareMsg); err != nil {
		logger.Errorf("Failed unmarshaling second last entry from WAL: %v", err)
		return err
	}

	prePrepareFromWAL := prePrepareMsg.Msg.GetPrePrepare()

	if prePrepareFromWAL == nil {
		return fmt.Errorf("expected second last message to be a pre-prepare, but got %v instead", prePrepareMsg)
	}

	if v.ProposalSequence < prePrepareFromWAL.Seq {
		err := fmt.Errorf("last proposal sequence persisted into WAL is %d which is greater than last committed sequence is %d", prePrepareFromWAL.Seq, v.ProposalSequence)
		logger.Errorf("Failed recovery: %s", err)
		return err
	}

	// Check if the WAL's last sequence has been persisted into the application layer.
	if v.ProposalSequence > prePrepareFromWAL.Seq {
		logger.Infof("Last proposal with sequence %d has been safely committed", v.ProposalSequence)
		return nil
	}

	// Else, v.ProposalSequence == prePrepareFromWAL.Seq

	prop := prePrepareFromWAL.Proposal
	v.inFlightProposal = &types.Proposal{
		VerificationSequence: int64(prop.VerificationSequence),
		Metadata:             prop.Metadata,
		Payload:              prop.Payload,
		Header:               prop.Header,
	}

	// Reconstruct the commit message we shall next broadcast
	// after the recovery.
	v.lastBroadcastSent = lastPersistedMessage
	v.Phase = PREPARED
	return nil
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestStateRestore(t *testing.T) {
	prePrepare := &protos.PrePrepare{
		Proposal: &protos.Proposal{
			Header:               []byte{1},
			Payload:              []byte{1},
			Metadata:             []byte{1},
			VerificationSequence: 100,
		},
		Seq:  200,
		View: 300,
	}

	expectedInFlightProposal := &types.Proposal{
		VerificationSequence: int64(prePrepare.Proposal.VerificationSequence),
		Metadata:             prePrepare.Proposal.Metadata,
		Payload:              prePrepare.Proposal.Payload,
		Header:               prePrepare.Proposal.Header,
	}

	proposedRecord := &protos.SavedMessage{
		Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: prePrepare,
				Prepare: &protos.Prepare{
					Seq:  200,
					View: 300,
				},
			},
		},
	}

	preparedProof := &protos.SavedMessage{
		Content: &protos.SavedMessage_PreparedProof{
			PreparedProof: &protos.PreparedProof{
				SignaturesByIds: map[uint64][]byte{
					11: {121},
					12: {144},
					13: {169},
				},
				Commit: &protos.Message{
					Content: &protos.Message_Commit{
						Commit: &protos.Commit{
							Seq:  200,
							View: 300,
							Signature: &protos.Signature{
								Signer: 11,
							},
						},
					},
				},
			},
		},
	}

	for _, testCase := range []struct {
		proposalSeqViewInitializedWith uint64
		description                    string
		WALContent                     [][]byte
		expectedError                  string
		expectedPhase                  bft.Phase
		expectedViewNumber             uint64
		expectedProposalSeq            uint64
		expectedInFlightProposal       *types.Proposal
	}{
		{
			description:        "empty",
			expectedViewNumber: 300,
		},
		{
			description: "malformed record",
			WALContent:  [][]byte{{1, 2, 3}},
			expectedError: "failed unmarshaling last entry from WAL:" +
				" proto: smartbftprotos.SavedMessage: illegal tag 0 (wire type 1)",
		},
		{
			description:   "unidentified record",
			WALContent:    [][]byte{nil},
			expectedError: "unrecognized record: ",
		},
		{
			description:              "proposed",
			expectedPhase:            bft.PROPOSED,
			expectedViewNumber:       300,
			expectedProposalSeq:      200,
			WALContent:               [][]byte{bft.MarshalOrPanic(proposedRecord)},
			expectedInFlightProposal: expectedInFlightProposal,
		},
		{
			description:   "commit persisted but pre-prepare nowhere to be found",
			expectedPhase: bft.PREPARED,
			WALContent: [][]byte{
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "last message is a commit, but expected to also have a matching pre-prepare",
		},
		{
			description:         "prepared but not committed",
			expectedPhase:       bft.PREPARED,
			expectedViewNumber:  300,
			expectedProposalSeq: 200,
			// Metadata holds sequence 199, WAL has sequence 200
			proposalSeqViewInitializedWith: 200,
			WALContent: [][]byte{
				bft.MarshalOrPanic(proposedRecord),
				bft.MarshalOrPanic(preparedProof),
			},
			expectedInFlightProposal: expectedInFlightProposal,
		},
		{
			description:         "prepared and committed",
			expectedPhase:       bft.COMMITTED,
			expectedViewNumber:  300,
			expectedProposalSeq: 201,
			// Metadata holds sequence 200 which is the same as WAL
			proposalSeqViewInitializedWith: 201,
			WALContent: [][]byte{
				bft.MarshalOrPanic(proposedRecord),
				bft.MarshalOrPanic(preparedProof),
			},
		},
		{
			description:   "WAL out of sync",
			expectedPhase: bft.COMMITTED,
			// Metadata holds sequence 198 but WAL has sequence 200
			proposalSeqViewInitializedWith: 199,
			WALContent: [][]byte{
				bft.MarshalOrPanic(proposedRecord),
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "last proposal sequence persisted into WAL is 200" +
				" which is greater than last committed sequence is 199",
		},
		{
			description:   "malformed prepared but valid commit",
			expectedPhase: bft.COMMITTED,
			WALContent: [][]byte{
				{1, 2, 3},
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "failed unmarshaling last entry from WAL: " +
				"proto: smartbftprotos.SavedMessage: illegal tag 0 (wire type 1)",
		},
		{
			description:   "malformed prepared but empty commit",
			expectedPhase: bft.COMMITTED,
			WALContent: [][]byte{
				bft.MarshalOrPanic(&protos.SavedMessage{Content: nil}),
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "expected second last message to be a pre-prepare, but got '' instead",
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			log := basicLog.Sugar()

			state := &bft.PersistedState{
				Entries:          testCase.WALContent,
				Logger:           log,
				InFlightProposal: &bft.InFlightProposal{},
			}

			view := &bft.View{
				Number:           300,
				ProposalSequence: testCase.proposalSeqViewInitializedWith,
			}

			err = state.Restore(view)

			if testCase.expectedError == "" {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expectedPhase, view.Phase)
				assert.Equal(t, testCase.expectedViewNumber, view.Number)
				assert.Equal(t, testCase.expectedProposalSeq, view.ProposalSequence)
				assert.Equal(t, testCase.expectedInFlightProposal, state.InFlightProposal.InFlightProposal())
			} else {
				assert.EqualError(t, err, testCase.expectedError)
			}
		})
	}
}

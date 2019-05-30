// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import "github.com/SmartBFT-Go/consensus/pkg/bft"

type Decider interface {
	Decide(proposal bft.Proposal, signatures []bft.Signature)
}

type FailureDetector interface {
	Complain()
}

type Synchronizer interface {
	SyncIfNeeded()
}

type View struct {
	Decider Decider
	FailureDetector FailureDetector
	Sync Synchronizer
}

func (v *View) Propose() {

}


func (v *View) Abort() {

}
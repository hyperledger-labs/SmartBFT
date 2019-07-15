// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math"

	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

func IsViewMessage(m *protos.Message) bool {
	return m.GetCommit() != nil || m.GetPrepare() != nil || m.GetPrePrepare() != nil
}

func viewNumber(m *protos.Message) uint64 {
	if pp := m.GetPrePrepare(); pp != nil {
		return pp.GetView()
	}

	if prp := m.GetPrepare(); prp != nil {
		return prp.GetView()
	}

	if cmt := m.GetCommit(); cmt != nil {
		return cmt.GetView()
	}

	return math.MaxUint64
}

func proposalSequence(m *protos.Message) uint64 {
	if pp := m.GetPrePrepare(); pp != nil {
		return pp.Seq
	}

	if prp := m.GetPrepare(); prp != nil {
		return prp.Seq
	}

	if cmt := m.GetCommit(); cmt != nil {
		return cmt.Seq
	}

	return math.MaxUint64
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

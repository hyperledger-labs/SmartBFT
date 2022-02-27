// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"sync"
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

func TestSimple(t *testing.T) {
	handlerWG := sync.WaitGroup{}
	handlerFunc := func(sender uint64, m *smartbftprotos.Message) { handlerWG.Done() }
	mh := newMessagesHandler(handlerFunc)
	mh.messages <- nil
	types.RegisterSanitizer(TypeMessageViewChange, nothingToSanitize)
	handlerWG.Add(3)
	vc := types.NewRecordedEvent(TypeMessageViewChange, nil)
	mh.messages <- []types.RecordedEvent{vc, vc, vc}
	handlerWG.Wait()
	mh.stop()
}

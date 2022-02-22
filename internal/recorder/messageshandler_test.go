// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"sync"
	"testing"
)

func TestSimple(t *testing.T) {
	doneWG := sync.WaitGroup{}
	doneFunc := func() {
		doneWG.Done()
	}
	handlerWG := sync.WaitGroup{}
	handlerFunc := func(sender uint64, m *smartbftprotos.Message) {handlerWG.Done()}
	mh := newMessagesHandler(handlerFunc, doneFunc)
	doneWG.Add(1)
	mh.messages <- nil
	doneWG.Wait()
	types.RegisterSanitizer(TypeMessageViewChange, nothingToSanitize)
	doneWG.Add(1)
	handlerWG.Add(3)
	vc := types.NewRecordedEvent(TypeMessageViewChange, nil)
	mh.messages <- []types.RecordedEvent{vc, vc, vc}
	handlerWG.Wait()
	doneWG.Wait()
	mh.stop()
}
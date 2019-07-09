// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import "github.com/SmartBFT-Go/consensus/smartbftprotos"

type StateRecorder struct {
	savedMessages []*smartbftprotos.Message
}

func (sr *StateRecorder) Save(message *smartbftprotos.Message) error {
	sr.savedMessages = append(sr.savedMessages, message)
	return nil
}

func (*StateRecorder) Restore() (*View, error) {
	panic("should not be used")
}

type PersistedState struct {
}

func (*PersistedState) Save(message *smartbftprotos.Message) error {
	panic("implement me")
}

func (*PersistedState) Restore() (*View, error) {
	panic("implement me")
}

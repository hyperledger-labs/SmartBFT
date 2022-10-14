// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// Generate mocks for a collection of interfaces that are defined in api/dependencies.go

//go:generate mockery -dir . -name VerifierMock -case underscore -output ./mocks/

// VerifierMock mock for the Verifier interface
type VerifierMock interface {
	api.Verifier
}

//go:generate mockery -dir . -name AssemblerMock -case underscore -output ./mocks/

// AssemblerMock mock for the Assembler interface
type AssemblerMock interface {
	api.Assembler
}

//go:generate mockery -dir . -name ApplicationMock -case underscore -output ./mocks/

// ApplicationMock mock for the Application interface
type ApplicationMock interface {
	api.Application
}

//go:generate mockery -dir . -name CommMock -case underscore -output ./mocks/

// CommMock mock for the Comm interface
type CommMock interface {
	api.Comm
	BroadcastConsensus(m *smartbftprotos.Message)
}

//go:generate mockery -dir . -name SynchronizerMock -case underscore -output ./mocks/

// SynchronizerMock mock for the Synchronizer interface
type SynchronizerMock interface {
	api.Synchronizer
}

//go:generate mockery -dir . -name SignerMock -case underscore -output ./mocks/

// SignerMock mock for the Signer interface
type SignerMock interface {
	api.Signer
}

//go:generate mockery -dir . -name MembershipNotifierMock -case underscore -output ./mocks/

// MembershipNotifierMock mock for the MembershipNotifier interface
type MembershipNotifierMock interface {
	api.MembershipNotifier
}

//go:generate mockery -dir . -name Synchronizer -case underscore -output ./mocks/

// Synchronizer mock for the Synchronizer interface (no return value)
type Synchronizer interface {
	Sync()
}

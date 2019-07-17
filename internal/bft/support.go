// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"github.com/SmartBFT-Go/consensus/pkg/api"
)

// Generate mocks for collection of interfaces that are also defined in api/dependencies.go

//go:generate mockery -dir . -name VerifierMock -case underscore -output ./mocks/
type VerifierMock interface {
	api.Verifier
}

//go:generate mockery -dir . -name AssemblerMock -case underscore -output ./mocks/
type AssemblerMock interface {
	api.Assembler
}

//go:generate mockery -dir . -name ApplicationMock -case underscore -output ./mocks/
type ApplicationMock interface {
	api.Application
}

//go:generate mockery -dir . -name CommMock -case underscore -output ./mocks/
type CommMock interface {
	api.Comm
}

//go:generate mockery -dir . -name SynchronizerMock -case underscore -output ./mocks/
type SynchronizerMock interface {
	api.Synchronizer
}

//go:generate mockery -dir . -name SignerMock -case underscore -output ./mocks/
type SignerMock interface {
	api.Signer
}

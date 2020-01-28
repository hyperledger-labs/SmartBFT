package test

import (
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/types"
)

type ConfigurationInt struct {
	// SelfID is added by the application
	RequestBatchMaxCount          int64
	RequestBatchMaxBytes          int64
	RequestBatchMaxInterval       time.Duration
	IncomingMessageBufferSize     int64
	RequestPoolSize               int64
	RequestForwardTimeout         time.Duration
	RequestComplainTimeout        time.Duration
	RequestAutoRemoveTimeout      time.Duration
	ViewChangeResendInterval      time.Duration
	ViewChangeTimeout             time.Duration
	LeaderHeartbeatTimeout        time.Duration
	LeaderHeartbeatCount          int64
	NumOfTicksBehindBeforeSyncing int64
	CollectTimeout                time.Duration
	SyncOnStart                   bool
	SpeedUpViewChange             bool
}

type ReconfigInt struct {
	InLatestDecision bool
	CurrentNodes     []int64
	CurrentConfig    ConfigurationInt
}

func (r ReconfigInt) recconfigToUint(id uint64) types.Reconfig {
	return types.Reconfig{
		InLatestDecision: r.InLatestDecision,
		CurrentNodes:     nodesToUint(r.CurrentNodes),
		CurrentConfig: types.Configuration{
			SelfID:                        id,
			RequestBatchMaxCount:          uint64(r.CurrentConfig.RequestBatchMaxCount),
			RequestBatchMaxBytes:          uint64(r.CurrentConfig.RequestBatchMaxBytes),
			RequestBatchMaxInterval:       r.CurrentConfig.RequestBatchMaxInterval,
			IncomingMessageBufferSize:     uint64(r.CurrentConfig.IncomingMessageBufferSize),
			RequestPoolSize:               uint64(r.CurrentConfig.RequestPoolSize),
			RequestForwardTimeout:         r.CurrentConfig.RequestForwardTimeout,
			RequestComplainTimeout:        r.CurrentConfig.RequestComplainTimeout,
			RequestAutoRemoveTimeout:      r.CurrentConfig.RequestAutoRemoveTimeout,
			ViewChangeResendInterval:      r.CurrentConfig.ViewChangeResendInterval,
			ViewChangeTimeout:             r.CurrentConfig.ViewChangeTimeout,
			LeaderHeartbeatTimeout:        r.CurrentConfig.LeaderHeartbeatTimeout,
			LeaderHeartbeatCount:          uint64(r.CurrentConfig.LeaderHeartbeatCount),
			NumOfTicksBehindBeforeSyncing: uint64(r.CurrentConfig.NumOfTicksBehindBeforeSyncing),
			CollectTimeout:                r.CurrentConfig.CollectTimeout,
			SyncOnStart:                   r.CurrentConfig.SyncOnStart,
			SpeedUpViewChange:             r.CurrentConfig.SpeedUpViewChange,
		},
	}
}

func recconfigToInt(reconfig types.Reconfig) ReconfigInt {
	return ReconfigInt{
		InLatestDecision: reconfig.InLatestDecision,
		CurrentNodes:     nodesToInt(reconfig.CurrentNodes),
		CurrentConfig: ConfigurationInt{
			RequestBatchMaxCount:          int64(reconfig.CurrentConfig.RequestBatchMaxCount),
			RequestBatchMaxBytes:          int64(reconfig.CurrentConfig.RequestBatchMaxBytes),
			RequestBatchMaxInterval:       reconfig.CurrentConfig.RequestBatchMaxInterval,
			IncomingMessageBufferSize:     int64(reconfig.CurrentConfig.IncomingMessageBufferSize),
			RequestPoolSize:               int64(reconfig.CurrentConfig.RequestPoolSize),
			RequestForwardTimeout:         reconfig.CurrentConfig.RequestForwardTimeout,
			RequestComplainTimeout:        reconfig.CurrentConfig.RequestComplainTimeout,
			RequestAutoRemoveTimeout:      reconfig.CurrentConfig.RequestAutoRemoveTimeout,
			ViewChangeResendInterval:      reconfig.CurrentConfig.ViewChangeResendInterval,
			ViewChangeTimeout:             reconfig.CurrentConfig.ViewChangeTimeout,
			LeaderHeartbeatTimeout:        reconfig.CurrentConfig.LeaderHeartbeatTimeout,
			LeaderHeartbeatCount:          int64(reconfig.CurrentConfig.LeaderHeartbeatCount),
			NumOfTicksBehindBeforeSyncing: int64(reconfig.CurrentConfig.NumOfTicksBehindBeforeSyncing),
			CollectTimeout:                reconfig.CurrentConfig.CollectTimeout,
			SyncOnStart:                   reconfig.CurrentConfig.SyncOnStart,
			SpeedUpViewChange:             reconfig.CurrentConfig.SpeedUpViewChange,
		},
	}
}

func nodesToUint(nodes []int64) []uint64 {
	nodesUint := make([]uint64, len(nodes))
	for i, v := range nodes {
		nodesUint[i] = uint64(v)
	}
	return nodesUint
}

func nodesToInt(nodes []uint64) []int64 {
	nodesUint := make([]int64, len(nodes))
	for i, v := range nodes {
		nodesUint[i] = int64(v)
	}
	return nodesUint
}

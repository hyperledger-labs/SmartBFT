package bft

import metrics "github.com/SmartBFT-Go/consensus/pkg/api"

var countOfRequestPoolOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_elements",
	Help:         "Count of request elements in pool for this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countOfFailAddRequestToPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_fail_add_request",
	Help:         "Count of fail add request in pool for this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

// ForwardTimeout
var countOfLeaderForwardRequestOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_leader_forward_request",
	Help:         "Count leader forward request on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countTimeoutTwoStepOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_timeout_two_step",
	Help:         "Count of timeout two step on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countOfDeleteRequestPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_delete_request",
	Help:         "Count of request delete from pool on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

type MetricsRequestPool struct {
	CountOfRequestPool          metrics.Gauge
	CountOfFailAddRequestToPool metrics.Counter
	CountOfLeaderForwardRequest metrics.Counter
	CountTimeoutTwoStep         metrics.Counter
	CountOfDeleteRequestPool    metrics.Counter
}

func NewMetricsRequestPool(p *metrics.CustomerProvider) *MetricsRequestPool {
	ch := p.Labels["channel"]

	return &MetricsRequestPool{
		CountOfRequestPool:          p.NewGauge(countOfRequestPoolOpts).With("channel", ch),
		CountOfFailAddRequestToPool: p.NewCounter(countOfFailAddRequestToPoolOpts).With("channel", ch),
		CountOfLeaderForwardRequest: p.NewCounter(countOfLeaderForwardRequestOpts).With("channel", ch),
		CountTimeoutTwoStep:         p.NewCounter(countTimeoutTwoStepOpts).With("channel", ch),
		CountOfDeleteRequestPool:    p.NewCounter(countOfDeleteRequestPoolOpts).With("channel", ch),
	}
}

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

var countOfRequestPoolAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_elements_all",
	Help:         "All of request elements in pool for this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var latencyOfRequestPoolOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_latency_of_elements",
	Help:         "Latency of request elements in pool for this channel.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

type MetricsRequestPool struct {
	CountOfRequestPool          metrics.Gauge
	CountOfFailAddRequestToPool metrics.Counter
	CountOfLeaderForwardRequest metrics.Counter
	CountTimeoutTwoStep         metrics.Counter
	CountOfDeleteRequestPool    metrics.Counter
	CountOfRequestPoolAll       metrics.Counter
	LatencyOfRequestPool        metrics.Histogram
}

func NewMetricsRequestPool(p *metrics.CustomerProvider) *MetricsRequestPool {
	ch := p.Labels["channel"]

	return &MetricsRequestPool{
		CountOfRequestPool:          p.NewGauge(countOfRequestPoolOpts).With("channel", ch),
		CountOfFailAddRequestToPool: p.NewCounter(countOfFailAddRequestToPoolOpts).With("channel", ch),
		CountOfLeaderForwardRequest: p.NewCounter(countOfLeaderForwardRequestOpts).With("channel", ch),
		CountTimeoutTwoStep:         p.NewCounter(countTimeoutTwoStepOpts).With("channel", ch),
		CountOfDeleteRequestPool:    p.NewCounter(countOfDeleteRequestPoolOpts).With("channel", ch),
		CountOfRequestPoolAll:       p.NewCounter(countOfRequestPoolAllOpts).With("channel", ch),
		LatencyOfRequestPool:        p.NewHistogram(latencyOfRequestPoolOpts).With("channel", ch),
	}
}

var countBlackListOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "blacklist_count",
	Help:         "Count of nodes in blacklist on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var nodesInBlackListOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "node_id_in_blacklist",
	Help:         "Node ID in blacklist on this channel.",
	LabelNames:   []string{"channel", "id"},
	StatsdFormat: "%{#fqname}.%{channel}.%{id}",
}

type MetricsBlacklist struct {
	CountBlackList   metrics.Gauge
	NodesInBlackList metrics.Gauge

	labels map[string]string
}

func NewMetricsBlacklist(p *metrics.CustomerProvider) *MetricsBlacklist {
	ch := p.Labels["channel"]

	return &MetricsBlacklist{
		CountBlackList:   p.NewGauge(countBlackListOpts).With("channel", ch),
		NodesInBlackList: p.NewGauge(nodesInBlackListOpts),
		labels:           p.Labels,
	}
}

func (m *MetricsBlacklist) Label(name string) string {
	return m.labels[name]
}

var consensusReconfigOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "consensus_reconfig",
	Help:         "count consensus reconfig on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

type MetricsReconfig struct {
	CountConsensusReconfig metrics.Counter
}

func NewMetricsReconfig(p *metrics.CustomerProvider) *MetricsReconfig {
	ch := p.Labels["channel"]

	return &MetricsReconfig{
		CountConsensusReconfig: p.NewCounter(consensusReconfigOpts).With("channel", ch),
	}
}

var viewNumberOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_number",
	Help:         "number of view on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var leaderIDOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_leader_id",
	Help:         "leader id on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var proposalSequenceOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_proposal_sequence",
	Help:         "proposal sequence view on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var decisionsInViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_decisions",
	Help:         "decisions in view on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var phaseOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_phase",
	Help:         "phase in view on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countTxsInBatchOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_txs_in_batch",
	Help:         "count txs in batch on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

type MetricsView struct {
	ViewNumber       metrics.Gauge
	LeaderID         metrics.Gauge
	ProposalSequence metrics.Gauge
	DecisionsInView  metrics.Gauge
	Phase            metrics.Gauge
	CountTxsInBatch  metrics.Gauge
}

func NewMetricsView(p *metrics.CustomerProvider) *MetricsView {
	ch := p.Labels["channel"]

	return &MetricsView{
		ViewNumber:       p.NewGauge(viewNumberOpts).With("channel", ch),
		LeaderID:         p.NewGauge(leaderIDOpts).With("channel", ch),
		ProposalSequence: p.NewGauge(proposalSequenceOpts).With("channel", ch),
		DecisionsInView:  p.NewGauge(decisionsInViewOpts).With("channel", ch),
		Phase:            p.NewGauge(phaseOpts).With("channel", ch),
		CountTxsInBatch:  p.NewGauge(countTxsInBatchOpts).With("channel", ch),
	}
}

var currentViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "viewchange_current_view",
	Help:         "current view of viewchange on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var nextViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "viewchange_next_view",
	Help:         "next view of viewchange on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var realViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "viewchange_real_view",
	Help:         "real view of viewchange on this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

type MetricsViewChange struct {
	CurrentView metrics.Gauge
	NextView    metrics.Gauge
	RealView    metrics.Gauge
}

func NewMetricsViewChange(p *metrics.CustomerProvider) *MetricsViewChange {
	ch := p.Labels["channel"]

	return &MetricsViewChange{
		CurrentView: p.NewGauge(currentViewOpts).With("channel", ch),
		NextView:    p.NewGauge(nextViewOpts).With("channel", ch),
		RealView:    p.NewGauge(realViewOpts).With("channel", ch),
	}
}

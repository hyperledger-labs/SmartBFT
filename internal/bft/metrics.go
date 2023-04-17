package bft

import metrics "github.com/SmartBFT-Go/consensus/pkg/api"

var countOfRequestPoolOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_elements",
	Help:         "Number of elements in the consensus request pool.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countOfFailAddRequestToPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_fail_add_request",
	Help:         "Number of requests pool insertion failure.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

// ForwardTimeout
var countOfLeaderForwardRequestOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_leader_forward_request",
	Help:         "Number of requests forwarded to the leader.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countTimeoutTwoStepOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_timeout_two_step",
	Help:         "Number of times requests reached second timeout.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countOfDeleteRequestPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_delete_request",
	Help:         "Number of elements removed from the request pool.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countOfRequestPoolAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_elements_all",
	Help:         "Total amount of elements in the request pool.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var latencyOfRequestPoolOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_latency_of_elements",
	Help:         "The average request processing time, time request resides in the pool.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

// MetricsRequestPool encapsulates request pool metrics
type MetricsRequestPool struct {
	CountOfRequestPool          metrics.Gauge
	CountOfFailAddRequestToPool metrics.Counter
	CountOfLeaderForwardRequest metrics.Counter
	CountTimeoutTwoStep         metrics.Counter
	CountOfDeleteRequestPool    metrics.Counter
	CountOfRequestPoolAll       metrics.Counter
	LatencyOfRequestPool        metrics.Histogram
}

// NewMetricsRequestPool create new request pool metrics
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
	StatsdFormat: "%{#fqname}.%{channel}.%{blackid}",
}

// MetricsBlacklist encapsulates blacklist metrics
type MetricsBlacklist struct {
	CountBlackList   metrics.Gauge
	NodesInBlackList metrics.Gauge

	labels map[string]string
}

// NewMetricsBlacklist create new blacklist metrics
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
	Help:         "Number of reconfiguration requests.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var latencySyncOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "consensus_latency_sync",
	Help:         "An average time it takes to sync node.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

// MetricsConsensus encapsulates consensus metrics
type MetricsConsensus struct {
	CountConsensusReconfig metrics.Counter
	LatencySync            metrics.Histogram
}

// NewMetricsConsensus create new consensus metrics
func NewMetricsConsensus(p *metrics.CustomerProvider) *MetricsConsensus {
	ch := p.Labels["channel"]

	return &MetricsConsensus{
		CountConsensusReconfig: p.NewCounter(consensusReconfigOpts).With("channel", ch),
		LatencySync:            p.NewHistogram(latencySyncOpts).With("channel", ch),
	}
}

var viewNumberOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_number",
	Help:         "The View number value.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var leaderIDOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_leader_id",
	Help:         "The leader id.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var proposalSequenceOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_proposal_sequence",
	Help:         "The sequence number within current view.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var decisionsInViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_decisions",
	Help:         "The number of decisions in the current view.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var phaseOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_phase",
	Help:         "Current consensus phase.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countTxsInBatchOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_txs_in_batch",
	Help:         "The number of transactions per batch.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countBatchAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_batch_all",
	Help:         "Amount of batched processed.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var countTxsAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_txs_all",
	Help:         "Total amount of transactions.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var sizeOfBatchOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_size_batch",
	Help:         "An average batch size.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var latencyBatchProcessingOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_latency_batch_processing",
	Help:         "Amount of time it take to process batch.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

var latencyBatchSaveOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_latency_batch_save",
	Help:         "An average time it takes to persist batch.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

// MetricsView encapsulates view metrics
type MetricsView struct {
	ViewNumber             metrics.Gauge
	LeaderID               metrics.Gauge
	ProposalSequence       metrics.Gauge
	DecisionsInView        metrics.Gauge
	Phase                  metrics.Gauge
	CountTxsInBatch        metrics.Gauge
	CountBatchAll          metrics.Counter
	CountTxsAll            metrics.Counter
	SizeOfBatch            metrics.Counter
	LatencyBatchProcessing metrics.Histogram
	LatencyBatchSave       metrics.Histogram
}

// NewMetricsView create new view metrics
func NewMetricsView(p *metrics.CustomerProvider) *MetricsView {
	ch := p.Labels["channel"]

	return &MetricsView{
		ViewNumber:             p.NewGauge(viewNumberOpts).With("channel", ch),
		LeaderID:               p.NewGauge(leaderIDOpts).With("channel", ch),
		ProposalSequence:       p.NewGauge(proposalSequenceOpts).With("channel", ch),
		DecisionsInView:        p.NewGauge(decisionsInViewOpts).With("channel", ch),
		Phase:                  p.NewGauge(phaseOpts).With("channel", ch),
		CountTxsInBatch:        p.NewGauge(countTxsInBatchOpts).With("channel", ch),
		CountBatchAll:          p.NewCounter(countBatchAllOpts).With("channel", ch),
		CountTxsAll:            p.NewCounter(countTxsAllOpts).With("channel", ch),
		SizeOfBatch:            p.NewCounter(sizeOfBatchOpts).With("channel", ch),
		LatencyBatchProcessing: p.NewHistogram(latencyBatchProcessingOpts).With("channel", ch),
		LatencyBatchSave:       p.NewHistogram(latencyBatchSaveOpts).With("channel", ch),
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

// MetricsViewChange encapsulates view change metrics
type MetricsViewChange struct {
	CurrentView metrics.Gauge
	NextView    metrics.Gauge
	RealView    metrics.Gauge
}

// NewMetricsViewChange create new view change metrics
func NewMetricsViewChange(p *metrics.CustomerProvider) *MetricsViewChange {
	ch := p.Labels["channel"]

	return &MetricsViewChange{
		CurrentView: p.NewGauge(currentViewOpts).With("channel", ch),
		NextView:    p.NewGauge(nextViewOpts).With("channel", ch),
		RealView:    p.NewGauge(realViewOpts).With("channel", ch),
	}
}

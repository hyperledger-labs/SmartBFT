package bft

import metrics "github.com/SmartBFT-Go/consensus/pkg/api"

const nameBlackListNodeID = "blackid"

var countOfRequestPoolOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_elements",
	Help:         "Number of elements in the consensus request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countOfFailAddRequestToPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_fail_add_request",
	Help:         "Number of requests pool insertion failure.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// ForwardTimeout
var countOfLeaderForwardRequestOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_leader_forward_request",
	Help:         "Number of requests forwarded to the leader.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countTimeoutTwoStepOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_timeout_two_step",
	Help:         "Number of times requests reached second timeout.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countOfDeleteRequestPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_delete_request",
	Help:         "Number of elements removed from the request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countOfRequestPoolAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_count_of_elements_all",
	Help:         "Total amount of elements in the request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencyOfRequestPoolOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "pool_latency_of_elements",
	Help:         "The average request processing time, time request resides in the pool.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
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
	countOfRequestPoolOpts.LabelNames = p.MakeLabelNames(countOfRequestPoolOpts.LabelNames...)
	countOfRequestPoolOpts.StatsdFormat = p.MakeStatsdFormat(countOfRequestPoolOpts.StatsdFormat)
	countOfFailAddRequestToPoolOpts.LabelNames = p.MakeLabelNames(countOfFailAddRequestToPoolOpts.LabelNames...)
	countOfFailAddRequestToPoolOpts.StatsdFormat = p.MakeStatsdFormat(countOfFailAddRequestToPoolOpts.StatsdFormat)
	countOfLeaderForwardRequestOpts.LabelNames = p.MakeLabelNames(countOfLeaderForwardRequestOpts.LabelNames...)
	countOfLeaderForwardRequestOpts.StatsdFormat = p.MakeStatsdFormat(countOfLeaderForwardRequestOpts.StatsdFormat)
	countTimeoutTwoStepOpts.LabelNames = p.MakeLabelNames(countTimeoutTwoStepOpts.LabelNames...)
	countTimeoutTwoStepOpts.StatsdFormat = p.MakeStatsdFormat(countTimeoutTwoStepOpts.StatsdFormat)
	countOfDeleteRequestPoolOpts.LabelNames = p.MakeLabelNames(countOfDeleteRequestPoolOpts.LabelNames...)
	countOfDeleteRequestPoolOpts.StatsdFormat = p.MakeStatsdFormat(countOfDeleteRequestPoolOpts.StatsdFormat)
	countOfRequestPoolAllOpts.LabelNames = p.MakeLabelNames(countOfRequestPoolAllOpts.LabelNames...)
	countOfRequestPoolAllOpts.StatsdFormat = p.MakeStatsdFormat(countOfRequestPoolAllOpts.StatsdFormat)
	latencyOfRequestPoolOpts.LabelNames = p.MakeLabelNames(latencyOfRequestPoolOpts.LabelNames...)
	latencyOfRequestPoolOpts.StatsdFormat = p.MakeStatsdFormat(latencyOfRequestPoolOpts.StatsdFormat)
	return &MetricsRequestPool{
		CountOfRequestPool:          p.NewGauge(countOfRequestPoolOpts).With(p.LabelsForWith()...),
		CountOfFailAddRequestToPool: p.NewCounter(countOfFailAddRequestToPoolOpts).With(p.LabelsForWith()...),
		CountOfLeaderForwardRequest: p.NewCounter(countOfLeaderForwardRequestOpts).With(p.LabelsForWith()...),
		CountTimeoutTwoStep:         p.NewCounter(countTimeoutTwoStepOpts).With(p.LabelsForWith()...),
		CountOfDeleteRequestPool:    p.NewCounter(countOfDeleteRequestPoolOpts).With(p.LabelsForWith()...),
		CountOfRequestPoolAll:       p.NewCounter(countOfRequestPoolAllOpts).With(p.LabelsForWith()...),
		LatencyOfRequestPool:        p.NewHistogram(latencyOfRequestPoolOpts).With(p.LabelsForWith()...),
	}
}

var countBlackListOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "blacklist_count",
	Help:         "Count of nodes in blacklist on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var nodesInBlackListOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "node_id_in_blacklist",
	Help:         "Node ID in blacklist on this channel.",
	LabelNames:   []string{nameBlackListNodeID},
	StatsdFormat: "%{#fqname}.%{" + nameBlackListNodeID + "}",
}

// MetricsBlacklist encapsulates blacklist metrics
type MetricsBlacklist struct {
	CountBlackList   metrics.Gauge
	NodesInBlackList metrics.Gauge

	labels []string
}

// NewMetricsBlacklist create new blacklist metrics
func NewMetricsBlacklist(p *metrics.CustomerProvider) *MetricsBlacklist {
	countBlackListOpts.LabelNames = p.MakeLabelNames(countBlackListOpts.LabelNames...)
	countBlackListOpts.StatsdFormat = p.MakeStatsdFormat(countBlackListOpts.StatsdFormat)
	nodesInBlackListOpts.LabelNames = p.MakeLabelNames(nodesInBlackListOpts.LabelNames...)
	nodesInBlackListOpts.StatsdFormat = p.MakeStatsdFormat(nodesInBlackListOpts.StatsdFormat)
	return &MetricsBlacklist{
		CountBlackList:   p.NewGauge(countBlackListOpts).With(p.LabelsForWith()...),
		NodesInBlackList: p.NewGauge(nodesInBlackListOpts),
		labels:           p.LabelsForWith(),
	}
}

func (m *MetricsBlacklist) LabelsForWith(labelValues ...string) []string {
	result := make([]string, 0, len(m.labels)+len(labelValues))
	result = append(result, labelValues...)
	result = append(result, m.labels...)
	return result
}

var consensusReconfigOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "consensus_reconfig",
	Help:         "Number of reconfiguration requests.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencySyncOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "consensus_latency_sync",
	Help:         "An average time it takes to sync node.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsConsensus encapsulates consensus metrics
type MetricsConsensus struct {
	CountConsensusReconfig metrics.Counter
	LatencySync            metrics.Histogram
}

// NewMetricsConsensus create new consensus metrics
func NewMetricsConsensus(p *metrics.CustomerProvider) *MetricsConsensus {
	consensusReconfigOpts.LabelNames = p.MakeLabelNames(consensusReconfigOpts.LabelNames...)
	consensusReconfigOpts.StatsdFormat = p.MakeStatsdFormat(consensusReconfigOpts.StatsdFormat)
	latencySyncOpts.LabelNames = p.MakeLabelNames(latencySyncOpts.LabelNames...)
	latencySyncOpts.StatsdFormat = p.MakeStatsdFormat(latencySyncOpts.StatsdFormat)
	return &MetricsConsensus{
		CountConsensusReconfig: p.NewCounter(consensusReconfigOpts).With(p.LabelsForWith()...),
		LatencySync:            p.NewHistogram(latencySyncOpts).With(p.LabelsForWith()...),
	}
}

var viewNumberOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_number",
	Help:         "The View number value.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var leaderIDOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_leader_id",
	Help:         "The leader id.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var proposalSequenceOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_proposal_sequence",
	Help:         "The sequence number within current view.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var decisionsInViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_decisions",
	Help:         "The number of decisions in the current view.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var phaseOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_phase",
	Help:         "Current consensus phase.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countTxsInBatchOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_txs_in_batch",
	Help:         "The number of transactions per batch.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countBatchAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_batch_all",
	Help:         "Amount of batched processed.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countTxsAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_count_txs_all",
	Help:         "Total amount of transactions.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var sizeOfBatchOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_size_batch",
	Help:         "An average batch size.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencyBatchProcessingOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_latency_batch_processing",
	Help:         "Amount of time it take to process batch.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencyBatchSaveOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "view_latency_batch_save",
	Help:         "An average time it takes to persist batch.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
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
	viewNumberOpts.LabelNames = p.MakeLabelNames(viewNumberOpts.LabelNames...)
	viewNumberOpts.StatsdFormat = p.MakeStatsdFormat(viewNumberOpts.StatsdFormat)
	leaderIDOpts.LabelNames = p.MakeLabelNames(leaderIDOpts.LabelNames...)
	leaderIDOpts.StatsdFormat = p.MakeStatsdFormat(leaderIDOpts.StatsdFormat)
	proposalSequenceOpts.LabelNames = p.MakeLabelNames(proposalSequenceOpts.LabelNames...)
	proposalSequenceOpts.StatsdFormat = p.MakeStatsdFormat(proposalSequenceOpts.StatsdFormat)
	decisionsInViewOpts.LabelNames = p.MakeLabelNames(decisionsInViewOpts.LabelNames...)
	decisionsInViewOpts.StatsdFormat = p.MakeStatsdFormat(decisionsInViewOpts.StatsdFormat)
	phaseOpts.LabelNames = p.MakeLabelNames(phaseOpts.LabelNames...)
	phaseOpts.StatsdFormat = p.MakeStatsdFormat(phaseOpts.StatsdFormat)
	countTxsInBatchOpts.LabelNames = p.MakeLabelNames(countTxsInBatchOpts.LabelNames...)
	countTxsInBatchOpts.StatsdFormat = p.MakeStatsdFormat(countTxsInBatchOpts.StatsdFormat)
	countBatchAllOpts.LabelNames = p.MakeLabelNames(countBatchAllOpts.LabelNames...)
	countBatchAllOpts.StatsdFormat = p.MakeStatsdFormat(countBatchAllOpts.StatsdFormat)
	countTxsAllOpts.LabelNames = p.MakeLabelNames(countTxsAllOpts.LabelNames...)
	countTxsAllOpts.StatsdFormat = p.MakeStatsdFormat(countTxsAllOpts.StatsdFormat)
	sizeOfBatchOpts.LabelNames = p.MakeLabelNames(sizeOfBatchOpts.LabelNames...)
	sizeOfBatchOpts.StatsdFormat = p.MakeStatsdFormat(sizeOfBatchOpts.StatsdFormat)
	latencyBatchProcessingOpts.LabelNames = p.MakeLabelNames(latencyBatchProcessingOpts.LabelNames...)
	latencyBatchProcessingOpts.StatsdFormat = p.MakeStatsdFormat(latencyBatchProcessingOpts.StatsdFormat)
	latencyBatchSaveOpts.LabelNames = p.MakeLabelNames(latencyBatchSaveOpts.LabelNames...)
	latencyBatchSaveOpts.StatsdFormat = p.MakeStatsdFormat(latencyBatchSaveOpts.StatsdFormat)
	return &MetricsView{
		ViewNumber:             p.NewGauge(viewNumberOpts).With(p.LabelsForWith()...),
		LeaderID:               p.NewGauge(leaderIDOpts).With(p.LabelsForWith()...),
		ProposalSequence:       p.NewGauge(proposalSequenceOpts).With(p.LabelsForWith()...),
		DecisionsInView:        p.NewGauge(decisionsInViewOpts).With(p.LabelsForWith()...),
		Phase:                  p.NewGauge(phaseOpts).With(p.LabelsForWith()...),
		CountTxsInBatch:        p.NewGauge(countTxsInBatchOpts).With(p.LabelsForWith()...),
		CountBatchAll:          p.NewCounter(countBatchAllOpts).With(p.LabelsForWith()...),
		CountTxsAll:            p.NewCounter(countTxsAllOpts).With(p.LabelsForWith()...),
		SizeOfBatch:            p.NewCounter(sizeOfBatchOpts).With(p.LabelsForWith()...),
		LatencyBatchProcessing: p.NewHistogram(latencyBatchProcessingOpts).With(p.LabelsForWith()...),
		LatencyBatchSave:       p.NewHistogram(latencyBatchSaveOpts).With(p.LabelsForWith()...),
	}
}

var currentViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "viewchange_current_view",
	Help:         "current view of viewchange on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var nextViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "viewchange_next_view",
	Help:         "next view of viewchange on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var realViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "viewchange_real_view",
	Help:         "real view of viewchange on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsViewChange encapsulates view change metrics
type MetricsViewChange struct {
	CurrentView metrics.Gauge
	NextView    metrics.Gauge
	RealView    metrics.Gauge
}

// NewMetricsViewChange create new view change metrics
func NewMetricsViewChange(p *metrics.CustomerProvider) *MetricsViewChange {
	currentViewOpts.LabelNames = p.MakeLabelNames(currentViewOpts.LabelNames...)
	currentViewOpts.StatsdFormat = p.MakeStatsdFormat(currentViewOpts.StatsdFormat)
	nextViewOpts.LabelNames = p.MakeLabelNames(nextViewOpts.LabelNames...)
	nextViewOpts.StatsdFormat = p.MakeStatsdFormat(nextViewOpts.StatsdFormat)
	realViewOpts.LabelNames = p.MakeLabelNames(realViewOpts.LabelNames...)
	realViewOpts.StatsdFormat = p.MakeStatsdFormat(realViewOpts.StatsdFormat)
	return &MetricsViewChange{
		CurrentView: p.NewGauge(currentViewOpts).With(p.LabelsForWith()...),
		NextView:    p.NewGauge(nextViewOpts).With(p.LabelsForWith()...),
		RealView:    p.NewGauge(realViewOpts).With(p.LabelsForWith()...),
	}
}

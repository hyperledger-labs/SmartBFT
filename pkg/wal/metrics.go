package wal

import metrics "github.com/SmartBFT-Go/consensus/pkg/api"

var countOfFilesOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "count_of_files",
	Help:         "Count of files in this channel.",
	LabelNames:   []string{"channel"},
	StatsdFormat: "%{#fqname}.%{channel}",
}

type Metrics struct {
	CountOfFiles metrics.Gauge
}

func NewMetrics(p *metrics.CustomerProvider) *Metrics {
	ch := p.Labels["channel"]

	return &Metrics{
		CountOfFiles: p.NewGauge(countOfFilesOpts).With("channel", ch),
	}
}

package api_test

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/stretchr/testify/assert"
)

var countOpts = api.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "count_of_files",
	Help:         "Count.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var nodesOpts = api.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "bft",
	Name:         "node",
	Help:         "Node ID.",
	LabelNames:   []string{"id"},
	StatsdFormat: "%{#fqname}.%{id}",
}

func TestMakeStatsdFormat(t *testing.T) {
	countOptsTmp := api.NewGaugeOpts(countOpts, []string{"label2", "label1"})
	nodesOptsTmp := api.NewGaugeOpts(nodesOpts, []string{"label2", "label1"})
	assert.Equal(t, "%{#fqname}.%{label1}.%{label2}", countOptsTmp.StatsdFormat)
	assert.Equal(t, "%{#fqname}.%{id}.%{label1}.%{label2}", nodesOptsTmp.StatsdFormat)
}

func TestMakeLabelNames(t *testing.T) {
	countOptsTmp := api.NewGaugeOpts(countOpts, []string{"label2", "label1"})
	nodesOptsTmp := api.NewGaugeOpts(nodesOpts, []string{"label2", "label1"})
	assert.Equal(t, []string{"label1", "label2"}, countOptsTmp.LabelNames)
	assert.Equal(t, []string{"id", "label1", "label2"}, nodesOptsTmp.LabelNames)
}

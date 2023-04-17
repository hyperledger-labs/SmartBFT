package api_test

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/metrics/disabled"
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
	cp := api.NewCustomerProvider(&disabled.Provider{}, "label2", "text1", "label1", "text2")
	countOptsTmp := cp.NewGaugeOpts(countOpts)
	nodesOptsTmp := cp.NewGaugeOpts(nodesOpts)
	assert.Equal(t, "%{#fqname}.%{label1}.%{label2}", countOptsTmp.StatsdFormat)
	assert.Equal(t, "%{#fqname}.%{id}.%{label1}.%{label2}", nodesOptsTmp.StatsdFormat)
}

func TestLabelsForWith(t *testing.T) {
	cp := api.NewCustomerProvider(&disabled.Provider{}, "label2", "text1", "label1", "text2")
	rez := cp.LabelsForWith()
	assert.Equal(t, []string{"label1", "text2", "label2", "text1"}, rez)
	rez = cp.LabelsForWith("id", "text3")
	assert.Equal(t, []string{"id", "text3", "label1", "text2", "label2", "text1"}, rez)
}

func TestMakeLabelNames(t *testing.T) {
	cp := api.NewCustomerProvider(&disabled.Provider{}, "label2", "text1", "label1", "text2")
	countOptsTmp := cp.NewGaugeOpts(countOpts)
	nodesOptsTmp := cp.NewGaugeOpts(nodesOpts)
	assert.Equal(t, []string{"label1", "label2"}, countOptsTmp.LabelNames)
	assert.Equal(t, []string{"id", "label1", "label2"}, nodesOptsTmp.LabelNames)
}

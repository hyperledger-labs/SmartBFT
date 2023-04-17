package api_test

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/metrics/disabled"
	"github.com/stretchr/testify/assert"
)

func TestMakeStatsdFormat(t *testing.T) {
	cp := api.NewCustomerProvider(&disabled.Provider{}, "label2", "text1", "label1", "text2")
	rez := cp.MakeStatsdFormat("%{#fqname}.%{id}")
	assert.Equal(t, "%{#fqname}.%{id}.%{label1}.%{label2}", rez)
	rez = cp.MakeStatsdFormat("%{#fqname}")
	assert.Equal(t, "%{#fqname}.%{label1}.%{label2}", rez)
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
	rez := cp.MakeLabelNames()
	assert.Equal(t, []string{"label1", "label2"}, rez)
	var s []string
	rez = cp.MakeLabelNames(s...)
	assert.Equal(t, []string{"label1", "label2"}, rez)
	rez = cp.MakeLabelNames("id")
	assert.Equal(t, []string{"id", "label1", "label2"}, rez)
}

/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package disabled

import (
	bft "github.com/SmartBFT-Go/consensus/pkg/api"
)

type Provider struct{}

func (p *Provider) NewCounter(o bft.CounterOpts) bft.Counter       { return &Counter{} }
func (p *Provider) NewGauge(o bft.GaugeOpts) bft.Gauge             { return &Gauge{} }
func (p *Provider) NewHistogram(o bft.HistogramOpts) bft.Histogram { return &Histogram{} }

type Counter struct{}

func (c *Counter) Add(delta float64) {}
func (c *Counter) With(labelValues ...string) bft.Counter {
	return c
}

type Gauge struct{}

func (g *Gauge) Add(delta float64) {}
func (g *Gauge) Set(delta float64) {}
func (g *Gauge) With(labelValues ...string) bft.Gauge {
	return g
}

type Histogram struct{}

func (h *Histogram) Observe(value float64) {}
func (h *Histogram) With(labelValues ...string) bft.Histogram {
	return h
}

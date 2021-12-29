package api

// Logger defines the contract for logging.
type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

// A Counter represents a monotonically increasing value.
type Counter interface {
	// With is used to provide label values when updating a Counter. This must be
	// used to provide values for all LabelNames provided to CounterOpts.
	With(labelValues ...string) Counter

	// Add increments a counter value.
	Add(delta float64)
}

// A Gauge is a meter that expresses the current value of some metric.
type Gauge interface {
	// With is used to provide label values when recording a Gauge value. This
	// must be used to provide values for all LabelNames provided to GaugeOpts.
	With(labelValues ...string) Gauge

	// Add increments a Gauge value.
	Add(delta float64)

	// Set is used to update the current value associated with a Gauge.
	Set(value float64)
}

// A Histogram is a meter that records an observed value into quantized
// buckets.
type Histogram interface {
	// With is used to provide label values when recording a Histogram
	// observation. This must be used to provide values for all LabelNames
	// provided to HistogramOpts.
	With(labelValues ...string) Histogram
	Observe(value float64)
}

// Diagnostics contains set of various metrics and logging interfaces
type Diagnostics struct {
	l Logger
	c Counter
	g Gauge
	h Histogram
}

// Set returns initialized Diagnostics
func (d Diagnostics) Set(
	l Logger,
	c Counter,
	g Gauge,
	h Histogram,
) Diagnostics {
	return Diagnostics{l: l, c: c, g: g, h: h}
}

// Update returns Diagnostics with only updated fields if arg is not nil
func (d Diagnostics) Update(
	l Logger,
	c Counter,
	g Gauge,
	h Histogram,
) (out Diagnostics) {
	if l != nil {
		out.l = l
	} else {
		out.l = d.l
	}

	if c != nil {
		out.c = c
	} else {
		out.c = d.c
	}

	if g != nil {
		out.g = g
	} else {
		out.g = d.g
	}

	if h != nil {
		out.h = h
	} else {
		out.h = d.h
	}

	return
}

// SetLogger returns new Diagnostics with updated Logger
func (d Diagnostics) SetLogger(l Logger) Diagnostics {
	d.l = l
	return d
}

// SetCounter returns new Diagnostics with updated Counter
func (d Diagnostics) SetCounter(c Counter) Diagnostics {
	d.c = c
	return d
}

// SetGauge returns new Diagnostics with updated Gauge
func (d Diagnostics) SetGauge(g Gauge) Diagnostics {
	d.g = g
	return d
}

// SetHistogram returns new Diagnostics with updated Histogram
func (d Diagnostics) SetHistogram(h Histogram) Diagnostics {
	d.h = h
	return d
}

// SetL returns new Diagnostics with updated Logger
func (d Diagnostics) SetL(l Logger) Diagnostics {
	return d.SetLogger(l)
}

// SetC returns new Diagnostics with updated Counter
func (d Diagnostics) SetC(c Counter) Diagnostics {
	return d.SetCounter(c)
}

// SetG returns new Diagnostics with updated Gauge
func (d Diagnostics) SetG(g Gauge) Diagnostics {
	return d.SetGauge(g)
}

// SetH returns new Diagnostics with updated Histogram
func (d Diagnostics) SetH(h Histogram) Diagnostics {
	return d.SetHistogram(h)
}

// Logger returns initialized Logger
func (d Diagnostics) Logger() Logger {
	if d.l != nil {
		return d.l
	}

	return EmptyLogger{}
}

// Counter returns initialized Counter
func (d Diagnostics) Counter() Counter {
	if d.c != nil {
		return d.c
	}

	return EmptyCounter{}
}

// Gauge returns initialized Gauge
func (d Diagnostics) Gauge() Gauge {
	if d.g != nil {
		return d.g
	}

	return EmptyGauge{}
}

// Histogram returns initialized Histogram
func (d Diagnostics) Histogram() Histogram {
	if d.h != nil {
		return d.h
	}

	return EmptyHistogram{}
}

// L returns initialized Logger
func (d Diagnostics) L() Logger {
	return d.Logger()
}

// C returns initialized Counter
func (d Diagnostics) C() Counter {
	return d.Counter()
}

// G returns initialized Gauge
func (d Diagnostics) G() Gauge {
	return d.Gauge()
}

// H returns initialized Histogram
func (d Diagnostics) H() Histogram {
	return d.Histogram()
}

// --------------------------------------

// EmptyLogger is used to prevent nil interface errors
type EmptyLogger struct{}

func (EmptyLogger) Debugf(string, ...interface{}) {}
func (EmptyLogger) Infof(string, ...interface{})  {}
func (EmptyLogger) Errorf(string, ...interface{}) {}
func (EmptyLogger) Warnf(string, ...interface{})  {}
func (EmptyLogger) Panicf(string, ...interface{}) {}

// EmptyCounter is used to prevent nil interface errors
type EmptyCounter struct{}

func (EmptyCounter) With(...string) Counter { return EmptyCounter{} }
func (EmptyCounter) Add(float64)            {}

// EmptyGauge is used to prevent nil interface errors
type EmptyGauge struct{}

func (EmptyGauge) With(...string) Gauge { return EmptyGauge{} }
func (EmptyGauge) Add(float64)          {}
func (EmptyGauge) Set(float64)          {}

// EmptyHistogram is used to prevent nil interface errors
type EmptyHistogram struct{}

func (EmptyHistogram) With(...string) Histogram { return &EmptyHistogram{} }
func (EmptyHistogram) Observe(float64)          {}

// --------------------------------------

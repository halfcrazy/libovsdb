package client

import (
	"github.com/ovn-kubernetes/libovsdb/model"
	"github.com/prometheus/client_golang/prometheus"
)

// cacheMetrics is an event handler that updates cache-related prometheus metrics
type cacheMetrics struct {
	dbName       string
	cacheEntries *prometheus.GaugeVec
}

// OnAdd implements the cache.EventHandler interface
func (c *cacheMetrics) OnAdd(table string, row model.Model) {
	c.cacheEntries.WithLabelValues(c.dbName, table).Inc()
}

// OnUpdate implements the cache.EventHandler interface
func (c *cacheMetrics) OnUpdate(table string, old, new model.Model) {
	// no-op, count doesn't change
}

// OnDelete implements the cache.EventHandler interface
func (c *cacheMetrics) OnDelete(table string, row model.Model) {
	c.cacheEntries.WithLabelValues(c.dbName, table).Dec()
}

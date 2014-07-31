package com.socrata.soda.server.metrics

import com.socrata.soda.server.metrics.Metrics.Metric

/**
 * Metric provider which does nothing.
 */
class NoopMetricProvider extends MetricProvider {
  def add(entity: String, metric: Metric) {}
}

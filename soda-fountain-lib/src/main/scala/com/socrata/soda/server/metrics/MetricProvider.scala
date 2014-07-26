package com.socrata.soda.server.metrics

import com.socrata.soda.server.metrics.Metrics._

/**
 *  Definition of a metric providing interface.
 */
trait MetricProvider {
  /**
   * Add a Metric to this MetricProvider.
   * @param entityId Entity that this Metric belongs to (ex: a domain)
   */
  def add(entityId: String, metric: Metric): Unit

  // Convenience method
  def add(entityId: Option[String], metric: Metric)(entityMissingHandler: (Metric => Unit)): Unit = {
    entityId.map(entity => add(entity, metric)).getOrElse(entityMissingHandler(metric))
  }
}
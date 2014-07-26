package com.socrata.soda.server.metrics

/**
 * Generic metrics for consumption by MetricProviders.
 */
object Metrics {
  sealed abstract class Metric(val id: String)
  // Count-based metrics
  class CountMetric(id: String, val count: Int = 1) extends Metric(id)

  // Metrics related to queries
  object QueryCacheHit extends CountMetric("sf-query-cache-hit")
  object QueryCacheMiss extends CountMetric("sf-query-cache-miss")
  object QuerySuccess extends CountMetric("sf-query-success")
  object QueryErrorUser extends CountMetric("sf-query-error-user")
  object QueryErrorInternal extends CountMetric("sf-query-error-internal")
  case class QueryRows(rowCount: Int) extends CountMetric("sf-query-rows", rowCount)
}


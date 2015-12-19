package com.socrata.soda.server.metrics

/**
 * Generic metrics for consumption by MetricProviders.
 */
object Metrics {
  sealed abstract class Metric(val id: String)
  // Count-based metrics
  class CountMetric(id: String, val count: Long = 1) extends Metric(id)

  // Metrics related to queries
  object QueryCacheHit extends CountMetric("sf-query-cache-hit")
  object QueryCacheMiss extends CountMetric("sf-query-cache-miss")
  object QuerySuccess extends CountMetric("sf-query-success")
  object QueryErrorUser extends CountMetric("sf-query-error-user")
  object QueryErrorInternal extends CountMetric("sf-query-error-internal")

  // Metrics related to geocoding
  sealed class GeocodeMetric(s: String, count: Long) extends CountMetric("geocode-" + s, count)
  object GeocodeMetric {
    def count(count: Int) = new GeocodeMetric("count", count)
    def cached(count: Int) = new GeocodeMetric("cached", count)
  }

  // Metrics related to geocoding using MapQuest
  sealed class MapQuestGeocodeMetric(s: String, count: Long) extends GeocodeMetric("mapquest-" + s, count)
  object MapQuestGeocodeMetric {
    def success(count: Long) = new MapQuestGeocodeMetric("success", count)
    def insufficientlyPreciseResult(count: Long) = new MapQuestGeocodeMetric("insufficiently-precise-result", count)
    def uninterpretableResult(count: Long) = new MapQuestGeocodeMetric("uninterpretable-result", count)
  }

  // Metrics related to computation handlers
  sealed class ComputationMetric(s: String, count: Long) extends CountMetric("computation-" + s, count)
  sealed abstract class ComputationMetrics(s: String) {
    def totalCount(count: Long) = new ComputationMetric(s + "-total-count", count)
    def failureCount(count: Long) = new ComputationMetric(s + "-failure-count", count)
    def milliseconds(count: Long) = new ComputationMetric(s + "-milliseconds", count)
  }

  object GeocodingHandlerMetric extends ComputationMetrics("geocoding")
  object GeoregionPointHandlerMetric extends ComputationMetrics("georegion-match-on-point")
  object GeoregionStringHandlerMetric extends ComputationMetrics("georegion-match-on-string")
}

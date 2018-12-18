package com.socrata.soda.server.metrics

import com.blist.metrics.impl.queue.MetricJmsQueueNotSingleton
import com.socrata.metrics.Fluff
import com.socrata.balboa.impl.AsyncActiveMQQueue

import com.socrata.soda.server.config.BalboaConfig
import com.socrata.soda.server.metrics.Metrics._

import java.io.Closeable

/**
 * Balboa MetricProvider implementation.
 * @param config Configuration containing connection information for balboa-client
 */
class BalboaMetricProvider(config: BalboaConfig) extends MetricProvider with Closeable {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[BalboaMetricProvider])
  private val metricQueue = new AsyncActiveMQQueue(config.activeMQConnectionUri, config.jmsQueue, 10)

  /**
   * Initializes necessary connections with external services.
   */
  def start() {
    metricQueue.start()
  }

  /**
   * Release current resources associated with this BalboaMetricProvider.
   */
  def close() {
    metricQueue.close()
  }

  /**
   * Add a Metric to Balboa.
   * @param entity Entity which this Metric belongs to (ex: a domain)
   * @param metric Metric to store. Currently, only CountMetrics are supported by BalboaMetricProvider.
   */
  def add(entity: String, metric: Metric) = {
    metric match {
      case countMetric: CountMetric =>
        log.debug(s"Metric received - count - entity: $entity, metricID: '${countMetric.id}', count: '${countMetric.count}'")
        metricQueue.create(Fluff(entity), Fluff(countMetric.id), countMetric.count)
    }
  }
}

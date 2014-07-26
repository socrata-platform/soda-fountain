package com.socrata.soda.server.metrics

import com.blist.metrics.impl.queue.MetricJmsQueueNotSingleton
import com.socrata.metrics.Fluff

import com.socrata.soda.server.config.BalboaConfig
import com.socrata.soda.server.metrics.Metrics._

import java.io.Closeable
import javax.jms.Connection

import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}
import org.apache.activemq.transport.DefaultTransportListener

import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

/**
 * Balboa MetricProvider implementation.
 * @param config Configuration containing connection information for balboa-client
 */
class BalboaMetricProvider(config: BalboaConfig) extends MetricProvider with Closeable {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[BalboaMetricProvider])
  private val activeMQFactory: ActiveMQConnectionFactory = {
    val factory = new ActiveMQConnectionFactory(config.activeMQConnectionUri)
    factory.setUseAsyncSend(true)
    factory
  }

  private var connection: Option[Connection] = None
  private var metricQueue: Option[MetricJmsQueueNotSingleton] = None
  private var started = false

  /**
   * Initializes necessary connections with external services. Must be called before add(). Should not be called again
   * without first calling close().
   */
  def start() = {
    if (!started) {
      started = true
      future { activeMQFactory.createConnection() } onComplete {
        case Success(conn) =>
          connection = Some({
            val activeMQConn = conn.asInstanceOf[ActiveMQConnection]
            activeMQConn.addTransportListener(BalboaTransportListener)
            activeMQConn
          })
          metricQueue = connection.map(new MetricJmsQueueNotSingleton(_, config.jmsQueue))
          metricQueue.map(_.start())
        case Failure(e) =>
          // Using failover transport, this should never happen; rather, createConnection() will hang until it finds a connection
          log.error("ActiveMQ initial connection failed. Metrics will not function until Soda Fountain restarts!", e)
      }
    } else {
      log.error("Cannot start BalboaMetricProvider... it is already started")
    }
  }

  /**
   * Release current resources associated with this BalboaMetricProvider.
   */
  def close() {
    val connected = BalboaTransportListener.isConnected
    connection.map(_.asInstanceOf[ActiveMQConnection].removeTransportListener(BalboaTransportListener))
    BalboaTransportListener.close()
    // If ActiveMQ is up, it can close down normally
    // If it is down, closing the queue can hang. So, kill the connection first
    try {
      if (connected) {
        try { metricQueue.map(_.close()) }
        finally { connection.map(_.close()) }
      } else {
        try { connection.map(_.close()) }
        finally { metricQueue.map(_.close())}
      }
    } finally {
      metricQueue = None
      connection = None
      started = false
    }
  }

  /**
   * Add a Metric to Balboa.
   * @param entity Entity which this Metric belongs to (ex: a domain)
   * @param metric Metric to store. Currently, only CountMetrics are supported by BalboaMetricProvider.
   */
  def add(entity: String, metric: Metric) = {
    metricQueue match {
      case Some(queue) if BalboaTransportListener.isConnected =>
        metric match {
          case countMetric: CountMetric =>
            log.debug(s"Metric received - count - entity: $entity, metricID: '${countMetric.id}', count: '${countMetric.count}'")
            queue.create(Fluff(entity), Fluff(countMetric.id), countMetric.count)
        }
      case Some(_) | None =>
        log.warn(s"Not connected to ActiveMQ. Is it up? Dropping metric - entity: '$entity', metricID: '${metric.id}'")
    }
  }

  /**
   * An ActiveMQ TransportListener that simply tracks the status of a connection.
   */
  private object BalboaTransportListener extends DefaultTransportListener with Closeable {
    private var jmsConnected = false

    override def transportInterupted() = {
      jmsConnected = false
      log.error("Connection to ActiveMQ lost. Metrics will start dropping")
    }

    override def transportResumed() = {
      jmsConnected = true
      log.info("Connection to ActiveMQ established.")
    }

    def isConnected: Boolean = jmsConnected

    def close() {
      jmsConnected = false
    }
  }
}
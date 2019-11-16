package com.socrata.soda.message

import java.util.concurrent.Executor
import java.util.Properties

import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.eurybates
import com.socrata.eurybates.zookeeper.ServiceConfiguration
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.zookeeper.ZooKeeperProvider
import com.typesafe.config.Config
import javax.jms.{Destination, Queue}
import org.slf4j.LoggerFactory

class EurybatesMessageProducer(sourceId: String,
                               zkp: ZooKeeperProvider,
                               executor: Executor,
                               properties: Properties,
                               optDestination: Option[Destination]) extends MessageProducer {
  val log = LoggerFactory.getLogger(classOf[EurybatesMessageProducer])

  private val producer = eurybates.Producer(sourceId, properties)


  private val serviceConfiguration = new ServiceConfiguration(zkp, executor, setServiceNames)

  def start(): Unit = {
    log.info(s"start source ${sourceId} queue ${optDestination}")
    producer.start()
    serviceConfiguration.start()
  }

  def setServiceNames(serviceNames: Set[String]): Unit = {
    producer.setServiceNames(serviceNames)
  }

  def shutdown(): Unit = {
    producer.stop()
  }

  def close(): Unit = {
    shutdown()
  }

  def send(message: Message, raw: Boolean = false): Unit = {
    log.debug("Sending message: {}", message)
    try {
      if (raw) producer.sendRaw(JsonEncode.toJValue(message), optDestination)
      else producer.send(eurybates.Message(EurybatesMessage.tag(message), JsonEncode.toJValue(message)))
    } catch {
      case e: IllegalStateException =>
        log.error("Failed to send message! Are there no open sessions to queues?", e)
    }
  }
}

class MessageProducerConfig(config: Config, root: String) extends ConfigClass(config, root) {
  def p(path: String) = root + "." + path // TODO: something better?
  val eurybates = new EurybatesConfig(config, p("eurybates"))
  val zookeeper = new ZookeeperConfig(config, p("zookeeper"))
  val queue = optionally(getString("queue"))
}

class EurybatesConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val producers = getString("producers")
  val activemqConnStr = getRawConfig("activemq").getString("connection-string")
}

class ZookeeperConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val connSpec = getString("conn-spec")
  val sessionTimeout = getDuration("session-timeout").toMillis.toInt
}

object MessageProducerFromConfig {
  def apply(id: String, executor: Executor, config: Option[MessageProducerConfig]): MessageProducer = config match {
    case Some(conf) =>
      val properties = new Properties()
      properties.setProperty("eurybates.producers", conf.eurybates.producers)
      properties.setProperty("eurybates.activemq.connection_string", conf.eurybates.activemqConnStr)
      val zkp = new ZooKeeperProvider(conf.zookeeper.connSpec, conf.zookeeper.sessionTimeout, executor)
      val optQueue = conf.queue.map { queueName =>
        new Queue() {
          def getQueueName: String = queueName
          override def toString = getQueueName
        }
      }
      new EurybatesMessageProducer(id, zkp, executor, properties, optQueue)
    case None => NoOpMessageProducer
  }
}

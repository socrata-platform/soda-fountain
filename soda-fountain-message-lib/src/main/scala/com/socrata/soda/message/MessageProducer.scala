package com.socrata.soda.message

import java.io.Closeable
import java.util.concurrent.Executor

import com.socrata.eurybates.activemq.ActiveMQServiceProducer.openActiveMQConnection
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config
import javax.jms.{Destination, Queue}


trait MessageProducer extends Closeable
{
  def start(): Unit
  def setServiceNames(serviceNames: Set[String]): Unit
  def send(message: Message, destination: Option[Destination] = None): Unit
}

object NoOpMessageProducer extends MessageProducer {
  def start(): Unit = {}
  def setServiceNames(serviceNames: Set[String]): Unit = {}
  def send(message: Message, destination: Option[Destination] = None): Unit = {}
  def close(): Unit = {}
}

class MessageProducerConfig(config: Config, root: String) extends ConfigClass(config, root) {
  def p(path: String) = root + "." + path
  val rawMessaging = new RawMessagingConfig(config, p("raw-messaging"))
}

class RawMessagingConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val producers = getString("producers")
  val activemqConnStr = getRawConfig("activemq").getString("connection-string")
  val queue = getString("queue")
}

class ZookeeperConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val connSpec = getString("conn-spec")
  val sessionTimeout = getDuration("session-timeout").toMillis.toInt
}

object MessageProducerFromConfig {
  def apply(id: String, executor: Executor, config: Option[MessageProducerConfig]): MessageProducer = config match {
    case Some(conf)  =>
      val optQueue =
        Some(new Queue() {
          def getQueueName: String = conf.rawMessaging.queue
          override def toString = conf.rawMessaging.queue
        })
      new ActiveMQServiceRawProducer(openActiveMQConnection(conf.rawMessaging.activemqConnStr), id, true, true, optQueue)
    case None => NoOpMessageProducer
  }
}

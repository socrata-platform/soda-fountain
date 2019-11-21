package com.socrata.soda.message

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.eurybates.activemq.ActiveMQServiceProducer
import com.socrata.util.logging.LazyStringLogger
import javax.jms.{Connection, Destination}

class ActiveMQServiceRawProducer(connection: Connection,
                                 sourceId: String,
                                 encodePrettily: Boolean = true,
                                 closeConnection: Boolean = false,
                                 defaultDestination: Option[Destination])
  extends ActiveMQServiceProducer(connection,
                                  sourceId,
                                  encodePrettily,
                                  closeConnection) with MessageProducer {

  private val log = new LazyStringLogger(getClass)

  def setServiceNames(serviceNames: Set[String]): Unit = {}

  def send(message: Message, destination: Option[Destination] = None): Unit = {
    session match {
      case Some(someSession) =>
        log.trace("Sending " + message)
        val encodedMessage = JsonUtil.renderJson(message, pretty = encodePrettily)
        val qMessage = someSession.createTextMessage(encodedMessage)
        destination.orElse(defaultDestination).foreach(target => producer.foreach(_.send(target, qMessage)))
      case None => throw new IllegalStateException("Session has not been started")
    }
  }

  def close(): Unit = {
    stop()
  }
}

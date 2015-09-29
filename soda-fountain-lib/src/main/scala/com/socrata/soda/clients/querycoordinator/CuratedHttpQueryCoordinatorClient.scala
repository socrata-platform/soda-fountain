package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.curator.CuratorServiceBase
import org.apache.curator.x.discovery.ServiceDiscovery
import scala.concurrent.duration.FiniteDuration
import com.socrata.http.common.AuxiliaryData
import java.io.Closeable

class CuratedHttpQueryCoordinatorClient(val httpClient: HttpClient,
                                        discovery: ServiceDiscovery[AuxiliaryData],
                                        serviceName: String,
                                        connectTimeout: FiniteDuration,
                                        receiveTimeout: FiniteDuration)
  extends CuratorServiceBase(discovery, serviceName) with HttpQueryCoordinatorClient
{
  private[this] val connectTimeoutMS = connectTimeout.toMillis.toInt
  if (connectTimeoutMS != connectTimeout.toMillis) {
    throw new IllegalArgumentException("Connect timeout out of range (milliseconds must fit in an int)")
  }

  private[this] val receiveTimeoutMS = receiveTimeout.toMillis.toInt
  if (receiveTimeoutMS != receiveTimeout.toMillis) {
    throw new IllegalArgumentException("Receive timeout out of range (milliseconds must fit in an int)")
  }

  def qchost: Option[RequestBuilder] = Option(provider.getInstance()).map { serv =>
    RequestBuilder(new java.net.URI(serv.buildUriSpec())).
      livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
      connectTimeoutMS(connectTimeoutMS).
      receiveTimeoutMS(receiveTimeoutMS)
  }
}

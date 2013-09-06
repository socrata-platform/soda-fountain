package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.netflix.curator.x.discovery.ServiceDiscovery
import com.netflix.curator.x.discovery.{strategies => providerStrategies}
import scala.concurrent.duration.FiniteDuration
import com.socrata.http.common.AuxiliaryData
import java.io.Closeable

class CuratedQueryCoordinatorClient(val internalHttpClient: HttpClient, discovery: ServiceDiscovery[AuxiliaryData], serviceName: String, connectTimeout: FiniteDuration) extends QueryCoordinatorClient with Closeable {
  private[this] val connectTimeoutMS = connectTimeout.toMillis.toInt
  if(connectTimeoutMS != connectTimeout.toMillis) throw new IllegalArgumentException("Connect timeout out of range (milliseconds must fit in an int)")

  val provider = discovery.serviceProviderBuilder().
    providerStrategy(new providerStrategies.RoundRobinStrategy).
    serviceName(serviceName).
    build()

  def start() {
    provider.start()
  }

  def close() {
    provider.close()
  }

  def qchost: Option[RequestBuilder] = Option(provider.getInstance()).map { serv =>
    RequestBuilder(new java.net.URI(serv.buildUriSpec())).
      livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
      connectTimeoutMS(connectTimeoutMS)
  }
}

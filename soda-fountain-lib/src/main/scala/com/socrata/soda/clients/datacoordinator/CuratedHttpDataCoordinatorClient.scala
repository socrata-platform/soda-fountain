package com.socrata.soda.clients.datacoordinator

import com.socrata.http.common.AuxiliaryData
import com.netflix.curator.x.discovery.{strategies => providerStrategies, ServiceDiscovery}
import com.socrata.http.client.{RequestBuilder, HttpClient}
import java.io.Closeable
import scala.concurrent.duration.FiniteDuration
import com.socrata.soda.server.util.ProviderCache

class CuratedHttpDataCoordinatorClient(httpClient: HttpClient,
                                       discovery: ServiceDiscovery[AuxiliaryData],
                                       serviceName: String,
                                       targetInstance: String,
                                       connectTimeout: FiniteDuration)
  extends HttpDataCoordinatorClient(httpClient) with Closeable
{
  private[this] val connectTimeoutMS = connectTimeout.toMillis.toInt
  if(connectTimeoutMS != connectTimeout.toMillis) throw new IllegalArgumentException("Connect timeout out of range (milliseconds must fit in an int)")

  val provider = new ProviderCache(discovery, new providerStrategies.RoundRobinStrategy, serviceName)

  def close() {
    provider.close()
  }

  def hostO(instance: String): Option[RequestBuilder] = Option(provider(instance).getInstance()).map { serv =>
    RequestBuilder(new java.net.URI(serv.buildUriSpec())).
      livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
      connectTimeoutMS(connectTimeoutMS)
  }
}

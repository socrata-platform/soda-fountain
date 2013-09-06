package com.socrata.soda.server.lowlevel

import com.socrata.http.common.AuxiliaryData
import com.netflix.curator.x.discovery.ServiceDiscovery
import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import java.io.Closeable
import com.netflix.curator.x.discovery.{strategies => providerStrategies}
import scala.concurrent.duration.FiniteDuration

class CuratedDataCoordinatorClient(val internalHttpClient: HttpClient,
                                   discovery: ServiceDiscovery[AuxiliaryData],
                                   serviceName: String,
                                   targetInstance: String,
                                   connectTimeout: FiniteDuration)
  extends DataCoordinatorClient with Closeable
{
  val provider = discovery.serviceProviderBuilder().
    providerStrategy(new providerStrategies.RoundRobinStrategy).
    serviceName(serviceName + "." + targetInstance).
    build()

  def start() { provider.start() }

  def close() {
    provider.close()
  }

  def hostO: Option[RequestBuilder] = Option(provider.getInstance()).map { serv =>
    RequestBuilder(new java.net.URI(serv.buildUriSpec())).
      livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
      connectTimeoutMS(connectTimeout.toMillis.toInt)
  }
}

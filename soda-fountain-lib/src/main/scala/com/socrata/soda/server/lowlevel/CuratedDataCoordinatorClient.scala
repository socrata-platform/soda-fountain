package com.socrata.soda.server.lowlevel

import com.socrata.http.common.AuxiliaryData
import com.netflix.curator.x.discovery.ServiceDiscovery
import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import java.io.Closeable
import com.netflix.curator.x.discovery.{strategies => providerStrategies}

class CuratedDataCoordinatorClient(discovery: ServiceDiscovery[AuxiliaryData], val internalHttpClient: HttpClient, serviceName: String, targetInstance: String) extends DataCoordinatorClient with Closeable {
  val provider = discovery.serviceProviderBuilder().
    providerStrategy(new providerStrategies.RoundRobinStrategy).
    serviceName(serviceName + "." + targetInstance).
    build()

  def start() { provider.start() }

  def close() {
    provider.close()
  }

  def hostO: Option[RequestBuilder] = Option(provider.getInstance()).map { serv =>
    RequestBuilder(new java.net.URI(serv.buildUriSpec()))
  }
}

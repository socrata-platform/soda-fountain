package com.socrata.soda.clients.datacoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.http.common.AuxiliaryData
import com.socrata.curator.ProviderCache
import java.io.Closeable
import com.socrata.soda.server.id.DatasetId
import org.apache.curator.x.discovery.{strategies => providerStrategies, ServiceDiscovery}
import scala.concurrent.duration.FiniteDuration

class CuratedHttpDataCoordinatorClient(httpClient: HttpClient,
                                       discovery: ServiceDiscovery[AuxiliaryData],
                                       discoveredInstances: () => Set[String],
                                       serviceName: String,
                                       targetInstance: String,
                                       connectTimeout: FiniteDuration,
                                       receiveTimeout: FiniteDuration)
  extends HttpDataCoordinatorClient(httpClient) with Closeable
{
  private[this] val connectTimeoutMS = connectTimeout.toMillis.toInt
  if (connectTimeoutMS != connectTimeout.toMillis) {
    throw new IllegalArgumentException("Connect timeout out of range (milliseconds must fit in an int)")
  }

  private[this] val receiveTimeoutMS = receiveTimeout.toMillis.toInt
  if (receiveTimeoutMS != receiveTimeout.toMillis) {
    throw new IllegalArgumentException("Receive timeout out of range (milliseconds must fit in an int)")
  }


  val provider = new ProviderCache(discovery, new providerStrategies.RoundRobinStrategy, serviceName)

  def close() {
    provider.close()
  }

  def hostO(instance: String): Option[RequestBuilder] = Option(provider(instance).getInstance()).map { serv =>
    RequestBuilder(new java.net.URI(serv.buildUriSpec())).
      livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
      connectTimeoutMS(connectTimeoutMS).
      receiveTimeoutMS(receiveTimeoutMS)
  }

  def instances() = discoveredInstances()
}

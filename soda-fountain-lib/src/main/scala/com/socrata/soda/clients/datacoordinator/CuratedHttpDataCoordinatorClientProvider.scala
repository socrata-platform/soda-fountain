package com.socrata.soda.clients.datacoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.http.server.util.RequestId.{RequestId, ReqIdHeader}
import com.socrata.http.common.AuxiliaryData
import com.socrata.curator.ProviderCache
import com.socrata.soda.server.{HeaderAddingHttpClient, SodaUtils, ThreadLimiter}
import java.io.Closeable
import com.socrata.soda.server.id.DatasetInternalName
import org.apache.curator.x.discovery.{strategies => providerStrategies, ServiceDiscovery}
import scala.concurrent.duration.FiniteDuration

class CuratedHttpDataCoordinatorClientProvider(discovery: ServiceDiscovery[AuxiliaryData],
                                               discoveredInstances: () => Set[String],
                                               serviceName: String,
                                               connectTimeout: FiniteDuration,
                                               receiveTimeout: FiniteDuration,
                                               maxJettyThreadPoolSize: Int,
                                               maxThreadRatio: Double)
  extends Closeable with (HttpClient => HttpDataCoordinatorClient)
{
  // Make sure the DC connection doesn't use all available threads
  val threadLimiter = new ThreadLimiter("DataCoordinatorClient",
                                        (maxThreadRatio * maxJettyThreadPoolSize).toInt)

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

  def apply(http: HttpClient): HttpDataCoordinatorClient =
    new HttpDataCoordinatorClient {
      val httpClient = http

      val threadLimiter = CuratedHttpDataCoordinatorClientProvider.this.threadLimiter

      def hostO(instance: String): Option[RequestBuilder] = Option(provider(instance).getInstance()).map { serv =>
        RequestBuilder(new java.net.URI(serv.buildUriSpec())).
          livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
          connectTimeoutMS(connectTimeoutMS).
          receiveTimeoutMS(receiveTimeoutMS)
      }

      def instances() = discoveredInstances()
    }
}

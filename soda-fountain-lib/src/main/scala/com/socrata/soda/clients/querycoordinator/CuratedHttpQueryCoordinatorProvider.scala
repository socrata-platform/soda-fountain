package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.http.server.util.RequestId.{RequestId, ReqIdHeader}
import com.socrata.soda.server.{HeaderAddingHttpClient, ThreadLimiter}
import com.socrata.curator.CuratorServiceBase
import org.apache.curator.x.discovery.ServiceDiscovery
import scala.concurrent.duration.FiniteDuration
import com.socrata.http.common.AuxiliaryData
import java.io.Closeable

class CuratedHttpQueryCoordinatorClientProvider(discovery: ServiceDiscovery[AuxiliaryData],
                                                serviceName: String,
                                                connectTimeout: FiniteDuration,
                                                receiveTimeout: FiniteDuration,
                                                maxJettyThreadPoolSize: Int,
                                                maxThreadRatio: Double
                                               )
  extends CuratorServiceBase(discovery, serviceName) with (HttpClient => HttpQueryCoordinatorClient)
{
  // Make sure the QC connection doesn't use all available threads
  val threadLimiter = new ThreadLimiter("QueryCoordinatorClient",
                                        (maxThreadRatio * maxJettyThreadPoolSize).toInt)

  private[this] val connectTimeoutMS = connectTimeout.toMillis.toInt
  if (connectTimeoutMS != connectTimeout.toMillis) {
    throw new IllegalArgumentException("Connect timeout out of range (milliseconds must fit in an int)")
  }

  private[this] val receiveTimeoutMS = receiveTimeout.toMillis.toInt
  if (receiveTimeoutMS != receiveTimeout.toMillis) {
    throw new IllegalArgumentException("Receive timeout out of range (milliseconds must fit in an int)")
  }


  def apply(http: HttpClient): HttpQueryCoordinatorClient = {
    new HttpQueryCoordinatorClient {
      val httpClient = http
      val threadLimiter = CuratedHttpQueryCoordinatorClientProvider.this.threadLimiter
      override val defaultReceiveTimeout: FiniteDuration = receiveTimeout

      def qchost: Option[RequestBuilder] = Option(provider.getInstance()).map { serv =>
        RequestBuilder(new java.net.URI(serv.buildUriSpec())).
          livenessCheckInfo(Option(serv.getPayload).flatMap(_.livenessCheckInfo)).
          connectTimeoutMS(connectTimeoutMS).
          receiveTimeoutMS(receiveTimeoutMS)
      }
    }
  }
}

package com.socrata.soda.server.lowlevel

import com.socrata.http.common.AuxiliaryData
import com.netflix.curator.x.discovery.{strategies => providerStrategies, ProviderStrategy, ServiceProvider, ServiceDiscovery}
import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import java.io.Closeable
import scala.concurrent.duration.FiniteDuration

class CuratedDataCoordinatorClient(val internalHttpClient: HttpClient,
                                   discovery: ServiceDiscovery[AuxiliaryData],
                                   serviceName: String,
                                   targetInstance: String,
                                   connectTimeout: FiniteDuration)
  extends DataCoordinatorClient with Closeable
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

class ProviderCache[T](discovery: ServiceDiscovery[T], strategy: ProviderStrategy[T], serviceName: String) {
  private[this] val prefix = serviceName + "."
  private[this] val serviceProviders = new java.util.concurrent.ConcurrentHashMap[String, ServiceProvider[T]]
  private[this] var closed = false

  def apply(instance: String): ServiceProvider[T] = {
    val serviceName = prefix + instance
    val existing = serviceProviders.get(serviceName)
    if(existing != null) return existing
    synchronized {
      if(closed) throw new IllegalStateException("ProviderCache closed")

      val secondTry = serviceProviders.get(serviceName)
      if(secondTry != null) return secondTry

      val newProvider = discovery.serviceProviderBuilder().
        providerStrategy(strategy).
        serviceName(serviceName).
        build()

      newProvider.start()

      try {
        serviceProviders.put(serviceName, newProvider)
      } catch {
        case e: Throwable =>
          newProvider.close()
          throw e
      }

      newProvider
    }
  }

  def close() {
    synchronized {
      closed = true
      var ex: Throwable = null
      val it = serviceProviders.entrySet.iterator
      while(it.hasNext) {
        val ent = it.next()
        it.remove()
        try { ent.getValue.close() }
        catch { case e: Throwable => ex = e }
      }
      if(ex != null) throw ex
    }
  }
}

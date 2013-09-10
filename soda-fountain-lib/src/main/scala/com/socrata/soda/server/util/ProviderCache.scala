package com.socrata.soda.server.util

import com.netflix.curator.x.discovery.{ServiceProvider, ProviderStrategy, ServiceDiscovery}

class ProviderCache[T](discovery: ServiceDiscovery[T], strategy: ProviderStrategy[T], serviceName: String) {
  private[this] val prefix = serviceName + "."
  private[this] val serviceProviders = new java.util.concurrent.ConcurrentHashMap[String, ServiceProvider[T]]
  @volatile private[this] var closed = false

  def apply(instance: String): ServiceProvider[T] = {
    if(closed) throw new IllegalStateException("ProviderCache closed")

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

      try {
        newProvider.start()
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
        catch { case e: Throwable =>
          if(ex != null) ex.addSuppressed(e)
          else ex = e
        }
      }
      serviceProviders.clear()
      if(ex != null) throw ex
    }
  }
}

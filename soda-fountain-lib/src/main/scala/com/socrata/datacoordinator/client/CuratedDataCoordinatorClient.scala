package com.socrata.datacoordinator.client

import com.socrata.soda.server.services.SodaService
import com.netflix.curator.x.discovery.{ServiceProvider, strategies}
import com.socrata.CuratorClient


trait CuratedDataCoordinatorClient extends SodaService with CuratorClient {
  val dc: DataCoordinatorClient = client

  private object client extends DataCoordinatorClient {

    private val config = SodaService.config.getConfig("data-coordinator-client")
    val serviceName = config.getString("service-name")
    val instanceName = config.getString("instance")
    val log = org.slf4j.LoggerFactory.getLogger(classOf[CuratedDataCoordinatorClient])

    lazy val provider : ServiceProvider[Void] = curatorClient.discovery.serviceProviderBuilder().providerStrategy(new strategies.RoundRobinStrategy).serviceName(serviceName + "." + instanceName).build()
    provider.start()

    def hostO: Option[String] = {
      try{
        Some(provider.getInstance().buildUriSpec())
      }
      catch {
        case e: Exception => log.error( "error finding data coordinator service endpoint", e)
        None
      }
    }
  }
}

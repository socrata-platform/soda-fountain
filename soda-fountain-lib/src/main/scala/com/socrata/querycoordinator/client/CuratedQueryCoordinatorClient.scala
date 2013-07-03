package com.socrata.querycoordinator.client

import com.socrata.soda.server.services.SodaService
import com.socrata.CuratorClient
import com.netflix.curator.x.discovery.{ServiceProvider, strategies}

trait CuratedQueryCoordinatorClient extends SodaService with CuratorClient{
  val qc: QueryCoordinatorClient  = client

  private object client extends QueryCoordinatorClient {

    val log = org.slf4j.LoggerFactory.getLogger(classOf[CuratedQueryCoordinatorClient])
    lazy val provider : ServiceProvider[Void] = curatorClient.discovery.serviceProviderBuilder().providerStrategy(new strategies.RoundRobinStrategy).serviceName("query-coordinator").build()
    provider.start()
    def qchost: Option[String] = {
      try{
        Some(provider.getInstance().buildUriSpec())
      }
      catch {
        case e: Exception => log.error( "error finding query coordinator service endpoint", e)
        None
      }
    }
  }
}

package com.socrata.querycoordinator.client

import com.socrata.soda.server.services.SodaService
import com.socrata.CuratorClient
import com.netflix.curator.x.discovery.{ServiceProvider, strategies}

trait CuratedQueryCoordinatorClient extends SodaService with CuratorClient{
  val qc: QueryCoordinatorClient  = client

  private object client extends QueryCoordinatorClient {
    def qchost : String = qcProvider.getInstance().buildUriSpec()
    def qcProvider : ServiceProvider[Void] = curatorClient.discovery.serviceProviderBuilder().providerStrategy(new strategies.RoundRobinStrategy).serviceName("data-coordinator").build()
  }
}

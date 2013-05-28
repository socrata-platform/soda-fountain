package com.socrata.querycoordinator.client

import com.socrata.soda.server.services.SodaService
import com.socrata.CuratorClient
import com.netflix.curator.x.discovery.{ServiceProvider, strategies}

trait CuratedQueryCoordinatorClient extends SodaService with CuratorClient{
  val qc: QueryCoordinatorClient  = client

  private object client extends QueryCoordinatorClient {

    var provider : ServiceProvider[Void] = null
    def qchost: String = {
      if (provider == null){
        provider = curatorClient.discovery.serviceProviderBuilder().providerStrategy(new strategies.RoundRobinStrategy).serviceName("query-coordinator").build()
        provider.start()
      }
      provider.getInstance().buildUriSpec()
    }
  }
}

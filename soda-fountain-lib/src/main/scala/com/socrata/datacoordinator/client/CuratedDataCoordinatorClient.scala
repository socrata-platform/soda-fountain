package com.socrata.datacoordinator.client

import com.socrata.soda.server.services.SodaService
import com.netflix.curator.x.discovery.{ServiceProvider, strategies}
import com.socrata.CuratorClient


trait CuratedDataCoordinatorClient extends SodaService with CuratorClient {
  val dc: DataCoordinatorClient = client

  private object client extends DataCoordinatorClient {
    def dcProvider : ServiceProvider[Void] = curatorClient.discovery.serviceProviderBuilder().providerStrategy(new strategies.RoundRobinStrategy).serviceName("data-coordinator").build()
    def baseUrl: String = dcProvider.getInstance().buildUriSpec()
  }
}

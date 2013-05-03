package com.socrata.datacoordinator.client

import com.socrata.soda.server.services.SodaService

trait CuratorClient extends SodaService {

  val dc: DataCoordinatorClient = client

  object client extends DataCoordinatorClient {
    def baseUrl: String = ???
  }

}

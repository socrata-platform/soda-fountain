package com.socrata.datacoordinator.client

import com.socrata.soda.server.services.SodaService

object CuratedDataCoordinatorClient  extends DataCoordinatorClient {
  def baseUrl: String = ???
}

trait CuratedDataCoordinatorClient extends SodaService {
  val dc: DataCoordinatorClient = CuratedDataCoordinatorClient
}

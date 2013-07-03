package com.socrata.soda.server.mocks

import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.services.SodaService

trait LocalDataCoordinator extends SodaService {

  val dc: DataCoordinatorClient = mock

  private object mock extends DataCoordinatorClient {
    def hostO = Some("http://localhost:12345")
  }
}

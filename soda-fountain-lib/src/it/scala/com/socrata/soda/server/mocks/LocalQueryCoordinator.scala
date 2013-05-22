package com.socrata.querycoordinator.client

import com.socrata.soda.server.services.SodaService

trait LocalQueryCoordinatorClient extends SodaService {

  val qc: QueryCoordinatorClient = LocalClient

  private object LocalClient extends QueryCoordinatorClient {
    val qchost = "localhost:54545"
  }
}

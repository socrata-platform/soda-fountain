package com.socrata.querycoordinator.client

import com.socrata.soda.server.services.SodaService
import com.socrata.InternalHttpClient

trait LocalQueryCoordinatorClient extends SodaService {

  val qc: QueryCoordinatorClient = LocalClient

  private object LocalClient extends QueryCoordinatorClient with InternalHttpClient {
    val qchost = Some("localhost:54545")
  }
}

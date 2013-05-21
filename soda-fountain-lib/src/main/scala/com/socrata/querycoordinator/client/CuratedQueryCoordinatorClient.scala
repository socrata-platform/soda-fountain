package com.socrata.querycoordinator.client

import com.socrata.soda.server.services.SodaService

object CuratedQueryCoordinatorClient  extends QueryCoordinatorClient {
  val qchost : String = ???
}
trait CuratedQueryCoordinatorClient extends SodaService {
  val qc = CuratedQueryCoordinatorClient
}

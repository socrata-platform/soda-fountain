package com.socrata.soda.server

import com.socrata.http.server._
import com.socrata.http.server.SocrataServerJetty
import com.socrata.soda.server.mocks._
import com.socrata.querycoordinator.client.LocalQueryCoordinatorClient
import com.socrata.soda.server.services.SodaService

object SodaFountainItegrationTestServerJetty {
  def main(args:Array[String]) {

    val fountain = new SodaFountain
      with MockNameAndSchemaStore
      with MockDaos
      with LocalDataCoordinator
      with LocalQueryCoordinatorClient
      with SodaRouter
    val server = new SocrataServerJetty(fountain.route, port = SodaService.config.getConfig("network").getInt("port"))
    server.run
  }
}



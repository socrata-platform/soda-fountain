package com.socrata.soda.server

import com.socrata.http.server._
import com.socrata.http.server.SocrataServerJetty
import com.socrata.soda.server.mocks.LocalDataCoordinator
import com.socrata.soda.server.mocks.MockNameAndSchemaStore
import com.socrata.soda.server.mocks.{LocalDataCoordinator, MockNameAndSchemaStore}
import com.socrata.querycoordinator.client.LocalQueryCoordinatorClient

object SodaFountainItegrationTestServerJetty {
  def main(args:Array[String]) {

    val fountain = new SodaFountain
      with MockNameAndSchemaStore
      with LocalDataCoordinator
      with LocalQueryCoordinatorClient
      with SodaRouter
    val server = new SocrataServerJetty(fountain.route, port = 8080)
    server.run
  }
}



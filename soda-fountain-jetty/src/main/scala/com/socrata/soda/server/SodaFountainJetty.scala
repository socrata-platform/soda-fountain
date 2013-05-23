package com.socrata.soda.server

import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

import java.io.FileInputStream
import java.util.Properties
import javax.servlet.http.HttpServletRequest
import com.socrata.datacoordinator.client.{CuratedDataCoordinatorClient, DataCoordinatorClient}
import com.socrata.soda.server.persistence.PostgresStore
import com.socrata.querycoordinator.client.CuratedQueryCoordinatorClient

object SodaFountainJetty {
  def main(args:Array[String]) {

    println("starting server")
    val fountain = new SodaFountain with PostgresStore with CuratedDataCoordinatorClient with CuratedQueryCoordinatorClient with SodaRouter
    val server = new SocrataServerJetty(fountain.route, port = 8080)

    fountain.curatorClient.open
    server.run
  }
}



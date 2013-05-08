package com.socrata.soda.server

import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

import java.io.FileInputStream
import java.util.Properties
import javax.servlet.http.HttpServletRequest
import com.socrata.datacoordinator.client.{CuratorClient, DataCoordinatorClient}
import com.socrata.soda.server.persistence.PostgresStore

object SodaFountainJetty {
  def main(args:Array[String]) {

    val fountain = new SodaFountain with PostgresStore with CuratorClient with SodaRouter
    val server = new SocrataServerJetty(fountain.route, port = 1950)
    server.run
  }
}



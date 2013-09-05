package com.socrata.soda.server

import com.rojoma.simplearm.util._
import com.typesafe.config.ConfigFactory
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.http.server.SocrataServerJetty

object SodaFountainJetty extends App {
  val config = new SodaFountainConfig(ConfigFactory.load())
  using(new SodaFountain(config)) { sodaFountain =>
    val server = new SocrataServerJetty(sodaFountain.router.route, port = config.network.port)
    server.run()
  }
}

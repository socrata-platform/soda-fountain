package com.socrata.soda.server

import com.rojoma.simplearm.util._
import com.typesafe.config.ConfigFactory
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.server.curator.CuratorBroker
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder

object SodaFountainJetty extends App {
  val config = new SodaFountainConfig(ConfigFactory.load())
  for {
    sodaFountain <- managed(new SodaFountain(config))
    discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[Void]).
      client(sodaFountain.curator).
      basePath(config.curator.serviceBasePath).
      build())
  } {
    discovery.start()

    val server = new SocrataServerJetty(
      sodaFountain.handle,
      port = config.network.port,
      broker = new CuratorBroker[Void](discovery, config.serviceAdvertisement.address, config.serviceAdvertisement.service, None))

    server.run()
  }
}

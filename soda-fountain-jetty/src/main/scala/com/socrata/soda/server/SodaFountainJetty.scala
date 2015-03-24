package com.socrata.soda.server

import com.rojoma.simplearm.util._
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.thirdparty.curator.DiscoveryFromConfig
import com.socrata.thirdparty.metrics.{SocrataHttpSupport, MetricsOptions, MetricsReporter}
import com.typesafe.config.ConfigFactory

object SodaFountainJetty extends App {
  val config = new SodaFountainConfig(ConfigFactory.load())
  val metricsOptions = MetricsOptions(config.codaMetrics)
  for {
    sodaFountain <- managed(new SodaFountain(config))
    discovery <- DiscoveryFromConfig(classOf[Void], sodaFountain.curator, config.discovery)
    reporter <- MetricsReporter.managed(metricsOptions)
  } {
    val server = new SocrataServerJetty(
      sodaFountain.handle,
      SocrataServerJetty.defaultOptions.
        withPort(config.network.port).
        withExtraHandlers(List(SocrataHttpSupport.getHandler(metricsOptions))).
        withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
        withBroker(new CuratorBroker[Void](
          discovery,
          config.discovery.address,
          config.discovery.name,
          None)))

    server.run()
  }
}

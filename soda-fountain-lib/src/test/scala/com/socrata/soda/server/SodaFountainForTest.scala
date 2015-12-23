package com.socrata.soda.server

import com.socrata.soda.server.config.SodaFountainConfig
import com.typesafe.config.ConfigFactory

object SodaFountainForTest extends SodaFountain(new SodaFountainConfig(ConfigFactory.load())) {

}
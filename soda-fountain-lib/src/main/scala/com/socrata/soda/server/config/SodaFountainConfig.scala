package com.socrata.soda.server.config

import com.typesafe.config.Config

class SodaFountainConfig(config: Config) extends ConfigClass(config, "com.socrata.soda-fountain") {
  val maxDatumSize = getInt("max-datum-size")
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val network = getConfig("network", new NetworkConfig(_, _))
  val log4j = getRawConfig("log4j")
}

class NetworkConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
}

class CuratorConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val ensemble = getStringList("ensemble").mkString(",")
  val sessionTimeout = getDuration("session-timeout")
  val connectTimeout = getDuration("connect-timeout")
  val maxRetries = getInt("max-retries")
  val baseRetryWait = getDuration("base-retry-wait")
  val maxRetryWait = getDuration("max-retry-wait")
  val namespace = getString("namespace")
  val serviceBasePath = getString("service-base-path")
}

package com.socrata.soda.server.config

import com.typesafe.config.Config

class SodaFountainConfig(config: Config) extends ConfigClass(config, "com.socrata.soda-fountain") {
  val maxDatumSize = getInt("max-datum-size")
  val curator = getConfig("curator", new CuratorConfig(_, _))
  val network = getConfig("network", new NetworkConfig(_, _))
  val dataCoordinatorClient = getConfig("data-coordinator-client", new DataCoordinatorClientConfig(_, _))
  val queryCoordinatorClient = getConfig("query-coordinator-client", new QueryCoordinatorClientConfig(_, _))
  val database = getConfig("database", new DataSourceConfig(_, _))
  val log4j = getRawConfig("log4j")
}

class DataCoordinatorClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val serviceName = getString("service-name")
  val instance = getString("instance")
  val connectTimeout = getDuration("connect-timeout")
}

class QueryCoordinatorClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val serviceName = getString("service-name")
  val connectTimeout = getDuration("connect-timeout")
}

class NetworkConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
  val httpclient = getConfig("client", new HttpClientConfig(_, _))
}

class HttpClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val liveness = getConfig("liveness", new LivenessClientConfig(_, _))
}

class LivenessClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val interval = getDuration("interval")
  val range = getDuration("range")
  val missable = getInt("missable")
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

class DataSourceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val host = getString("host")
  val port = getInt("port")
  val database = getString("database")
  val username = getString("username")
  val password = getString("password")
}

package com.socrata

import java.io.Closeable
import com.netflix.curator.retry
import com.netflix.curator.x.discovery.{ServiceDiscovery, strategies, ServiceProvider, ServiceDiscoveryBuilder}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.socrata.soda.server.services.SodaService
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.typesafe.config.Config

trait CuratorClient {

  private val config = SodaService.config.getConfig("curator")
  val curatorClient = new CuratorClientImpl(config)
}

class CuratorClientImpl(config: Config) extends Closeable {

  object CuratorConfig {
    val ensemble = config.getStringList("ensemble").asScala.mkString(",")
    val sessionTimeout = config.getMilliseconds("session-timeout").longValue.millis
    val connectTimeout = config.getMilliseconds("connect-timeout").longValue.millis
    val maxRetries = config.getInt("max-retries")
    val baseRetryWait = config.getMilliseconds("base-retry-wait").longValue.millis
    val maxRetryWait = config.getMilliseconds("max-retry-wait").longValue.millis
    val namespace = config.getString("namespace")
    val serviceBasePath = config.getString("service-base-path")
  }
  var curator: CuratorFramework = null
  var discovery: ServiceDiscovery[Void] = null

  def open = {
    curator = CuratorFrameworkFactory.builder.
    connectString(CuratorConfig.ensemble).
    sessionTimeoutMs(CuratorConfig.sessionTimeout.toMillis.toInt).
    connectionTimeoutMs(CuratorConfig.connectTimeout.toMillis.toInt).
    retryPolicy(new retry.BoundedExponentialBackoffRetry(CuratorConfig.baseRetryWait.toMillis.toInt,
    CuratorConfig.maxRetryWait.toMillis.toInt,
    CuratorConfig.maxRetries)).
    namespace(CuratorConfig.namespace).
    build()

    discovery = ServiceDiscoveryBuilder.builder(classOf[Void]).
    client(curator).
    basePath(CuratorConfig.serviceBasePath).
    build()

    curator.start()
    discovery.start()
  }

  def close = {
    curator.close()
    discovery.close()
  }
}

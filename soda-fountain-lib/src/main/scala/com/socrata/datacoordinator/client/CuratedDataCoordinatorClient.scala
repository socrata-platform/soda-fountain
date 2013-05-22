package com.socrata.datacoordinator.client

import com.socrata.soda.server.services.SodaService
import com.netflix.curator.x.discovery.{ServiceProvider, ServiceDiscoveryBuilder, ServiceDiscovery}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry
import com.rojoma.simplearm.util._
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.netflix.curator.x.discovery.strategies

object CuratedDataCoordinatorClient  extends DataCoordinatorClient {


  def baseUrl: String = dcProvider.getInstance().buildUriSpec()
  var dcProvider : ServiceProvider[Void] = null

  val config = SodaService.config.getConfig("curator")
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

  for {
    curator <- managed(CuratorFrameworkFactory.builder.
      connectString(CuratorConfig.ensemble).
      sessionTimeoutMs(CuratorConfig.sessionTimeout.toMillis.toInt).
      connectionTimeoutMs(CuratorConfig.connectTimeout.toMillis.toInt).
      retryPolicy(new retry.BoundedExponentialBackoffRetry(CuratorConfig.baseRetryWait.toMillis.toInt,
                                                           CuratorConfig.maxRetryWait.toMillis.toInt,
                                                           CuratorConfig.maxRetries)).
      namespace(CuratorConfig.namespace).
      build())
    discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[Void]).
      client(curator).
      basePath(CuratorConfig.serviceBasePath).
      build())
  } {
    curator.start()
    discovery.start()

    this.dcProvider = discovery.serviceProviderBuilder().providerStrategy(new strategies.RoundRobinStrategy).serviceName("data-coordinator").build()
    //TODO: better way to manage this? How about shutdown?
  }
}

trait CuratedDataCoordinatorClient extends SodaService {
  val dc: DataCoordinatorClient = CuratedDataCoordinatorClient
}

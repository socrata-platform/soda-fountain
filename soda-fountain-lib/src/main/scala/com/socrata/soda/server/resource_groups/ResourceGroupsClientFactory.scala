package com.socrata.soda.server.resource_groups

import com.socrata.resource_groups.client.{ResourceGroupsClient, ResourceGroupsClientBuilder, ResourceGroupsException}
import com.socrata.soda.server.config.SodaFountainConfig
import com.typesafe.config.ConfigFactory

object ResourceGroupsClientFactory {
  private val factory = {
    val configuration = new SodaFountainConfig(ConfigFactory.load()).resourceGroupsClient
    val apiHost = configuration.apiHost.orNull
    new ResourceGroupsClientFactory(apiHost)
  }

  private lazy val clientSingleton = factory._client()

  def client(): ResourceGroupsClient = clientSingleton
}

class ResourceGroupsClientFactory private (apiHost: String) {
  private def _client(): ResourceGroupsClient =
    ResourceGroupsClientBuilder.builder()
      .apiHost(apiHost)
      .httpClientAdapter(new ResourceGroupsHttpAdapter())
      .jsonCodecAdapter(new ResourceGroupsJsonAdapter())
      .build()
  }

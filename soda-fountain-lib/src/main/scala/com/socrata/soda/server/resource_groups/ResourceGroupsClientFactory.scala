package com.socrata.soda.server.resource_groups

import com.socrata.resource_groups.client.{ResourceGroupsClient, ResourceGroupsClientBuilder}
import com.socrata.soda.server.config.SodaFountainConfig

object ResourceGroupsClientFactory {
  private lazy val clientSingleton = factory._client()
  private val factory = {
    val configuration = SodaFountainConfig.config().resourceGroupsClient
    val apiHost = configuration.apiHost.orNull
    new ResourceGroupsClientFactory(apiHost)
  }

  def client(): ResourceGroupsClient = clientSingleton
}

class ResourceGroupsClientFactory private(apiHost: String) {
  private def _client(): ResourceGroupsClient =
    ResourceGroupsClientBuilder.builder()
      .apiHost(apiHost)
      .httpClientAdapter(new ResourceGroupsHttpAdapter())
      .jsonCodecAdapter(new ResourceGroupsJsonAdapter())
      .build()
}

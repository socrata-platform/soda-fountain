package com.socrata.soda.server.resource_groups

import com.socrata.resource_groups.client.{ResourceGroupsClient, ResourceGroupsClientBuilder}
import com.socrata.soda.server.config.{ResourceGroupClientConfig, SodaFountainConfig}


class ResourceGroupsClientFactory(resourceGroupClientConfig: ResourceGroupClientConfig) {
  private lazy val _client = ResourceGroupsClientBuilder.builder()
    .apiHost(resourceGroupClientConfig.apiHost.orNull)
    .httpClientAdapter(new ResourceGroupsHttpAdapter())
    .jsonCodecAdapter(new ResourceGroupsJsonAdapter())
    .build()

  def client(): ResourceGroupsClient = _client
}

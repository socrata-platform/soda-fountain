package com.socrata.soda.server.resource_groups

import com.socrata.http.client.HttpClient
import com.socrata.resource_groups.client.{ResourceGroupsClient, ResourceGroupsClientBuilder}
import com.socrata.soda.server.config.{ResourceGroupsClientConfig, SodaFountainConfig}


class ResourceGroupsClientFactory(resourceGroupClientConfig: ResourceGroupsClientConfig, httpClient: HttpClient) {
  private lazy val _client = ResourceGroupsClientBuilder.builder()
    .apiHost(resourceGroupClientConfig.apiHost.orNull)
    .httpClientAdapter(new ResourceGroupsHttpAdapter(httpClient))
    .jsonCodecAdapter(new ResourceGroupsJsonAdapter())
    .build()

  def client(): ResourceGroupsClient = _client
}

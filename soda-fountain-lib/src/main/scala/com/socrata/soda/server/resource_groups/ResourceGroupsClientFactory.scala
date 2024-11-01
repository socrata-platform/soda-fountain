package com.socrata.soda.server.resource_groups

import com.socrata.http.client.HttpClient
import com.socrata.resource_groups.client.{ResourceGroupsClient, ResourceGroupsClientBuilder, ResourceGroupsException}
import com.socrata.soda.server.config.{ResourceGroupsClientConfig, SodaFountainConfig}


class ResourceGroupsClientFactory(resourceGroupClientConfig: ResourceGroupsClientConfig, httpClient: HttpClient) {
  private lazy val _client = {
    val apiHost = resourceGroupClientConfig.apiHost match {
      case Some(theApiHost) => resourceGroupClientConfig.apiProtocol match {
        case Some(theApiProtocol) => s"${theApiProtocol}://${theApiHost}"
        case None => throw new ResourceGroupsException("apiHost was provided, but protocol is absent. You must provide a protocol.")
      }
      case None => null //null to trigger stub
    }
    println(s"Resource groups apiHost: '${apiHost}'")
    ResourceGroupsClientBuilder.builder()
      .apiHost(apiHost)
      .httpClientAdapter(new ResourceGroupsHttpAdapter(httpClient))
      .jsonCodecAdapter(new ResourceGroupsJsonAdapter())
      .build()
  }

  def client(): ResourceGroupsClient = _client
}

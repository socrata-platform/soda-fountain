package com.socrata.soda.server.resource_groups

import com.socrata.resource_groups.client.{ResourceGroupsClient,ResourceGroupsClientBuilder,ResourceGroupsException}

object ResourceGroupsClientFactory {
  private val factory = for {
    configuration <- Option(Environment.getConfiguration)
    apiHost <- Option(configuration.getProperty("resource_groups.api.host"))
  } yield new ResourceGroupsClientFactory(apiHost)

  private lazy val clientSingleton = factory.map(_._client()).getOrElse {
    //Lets force a fallback stub for now
    //null api host causes the builder to use a stub
    new ResourceGroupsClientFactory(null)._client()
  }

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

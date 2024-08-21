package com.socrata.soda.server.resource_groups

import com.socrata.resource_groups.client.ResourceGroupsClientBuilder
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType

import java.net.URI
import java.nio.charset.StandardCharsets


class ResourceGroupsHttpAdapter extends ResourceGroupsClientBuilder.HttpClientAdapter {

  override def get(uri: URI): String = {
    new String(Request.Get(uri).execute().returnContent().asBytes(), StandardCharsets.UTF_8)
  }

  override def post(uri: URI, s: String): String = {
    new String(Request.Post(uri).bodyString(s, ContentType.APPLICATION_JSON).execute().returnContent().asBytes(), StandardCharsets.UTF_8)
  }

  override def delete(uri: URI): String = {
    new String(Request.Delete(uri).execute().returnContent().asBytes(), StandardCharsets.UTF_8)
  }
}

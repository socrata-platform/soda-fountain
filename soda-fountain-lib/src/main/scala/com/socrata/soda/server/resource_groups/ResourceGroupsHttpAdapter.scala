package com.socrata.soda.server.resource_groups

import com.socrata.http.client.{HttpClient, RequestBuilder}
import com.socrata.resource_groups.client.ResourceGroupsClientBuilder
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.ReaderInputStream

import java.io.StringReader
import java.net.URI
import java.nio.charset.StandardCharsets


class ResourceGroupsHttpAdapter(httpClient: HttpClient) extends ResourceGroupsClientBuilder.HttpClientAdapter {

  override def get(uri: URI): String = {
    val req = RequestBuilder(uri).get
    for (resp <- httpClient.execute(req)) {
      IOUtils.toString(resp.reader(Some(StandardCharsets.UTF_8)))
    }
  }

  override def post(uri: URI, s: String): String = {
    val body = new ReaderInputStream(new StringReader(Option(s).getOrElse("")), StandardCharsets.UTF_8)
    val req = RequestBuilder(uri).blob(body, "application/json")
    for (resp <- httpClient.execute(req)) {
      IOUtils.toString(resp.reader(Some(StandardCharsets.UTF_8)))
    }
  }

  override def delete(uri: URI): String = {
    val req = RequestBuilder(uri).delete
    for (resp <- httpClient.execute(req)) {
      IOUtils.toString(resp.reader(Some(StandardCharsets.UTF_8)))
    }
  }
}

package com.socrata.soda.server.resource_groups

import com.socrata.http.client.{HttpClient, SimpleHttpRequest}

import java.io.Closeable

case class NoopHttpClient() extends HttpClient {
  override def executeRawUnmanaged(req: SimpleHttpRequest): RawResponse with Closeable = null

  override def close(): Unit = {}
}

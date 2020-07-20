package com.socrata.soda.server

import java.io.Closeable

import com.socrata.http.client._

class HeaderAddingHttpClient(underlying: HttpClient, extraHeaders: Iterable[(String, String)]) extends HttpClient {
  override def close() = underlying.close()

  override def executeRawUnmanaged(req: SimpleHttpRequest): RawResponse with Closeable = {
    new RawResponse with Closeable {
      val resp = underlying.executeRawUnmanaged(augment(req))

      override def close() = resp.close()
      override val body = resp.body
      override val responseInfo = resp.responseInfo
    }
  }

  private def augment(req: SimpleHttpRequest): SimpleHttpRequest = {
    req match {
      case bhr: BodylessHttpRequest =>
        new BodylessHttpRequest(bhr.builder.addHeaders(extraHeaders))
      case fhr: FormHttpRequest =>
        new FormHttpRequest(fhr.builder.addHeaders(extraHeaders), fhr.contents)
      case fhr: FileHttpRequest =>
        new FileHttpRequest(fhr.builder.addHeaders(extraHeaders), fhr.contents, fhr.file, fhr.field, fhr.contentType)
      case jhr: JsonHttpRequest =>
        new JsonHttpRequest(jhr.builder.addHeaders(extraHeaders), jhr.contents)
      case bhr: BlobHttpRequest =>
        new BlobHttpRequest(bhr.builder.addHeaders(extraHeaders), bhr.contents, bhr.contentType)
    }
  }
}

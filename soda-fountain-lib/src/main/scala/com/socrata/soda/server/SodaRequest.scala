package com.socrata.soda.server

import scala.language.implicitConversions

import com.socrata.http.server.{HttpRequest, GeneratedHttpRequestApi}
import com.socrata.http.server.util.RequestId.RequestId

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient

trait SodaRequest {
  val httpRequest: HttpRequest
  def resourceScope = httpRequest.resourceScope
}

object SodaRequest {
  implicit def generatedSodaRequestApi(req: SodaRequest): GeneratedHttpRequestApi =
    req.httpRequest

  implicit def sodaRequestApi(req: SodaRequest): HttpRequest.HttpRequestApi =
    req.httpRequest
}

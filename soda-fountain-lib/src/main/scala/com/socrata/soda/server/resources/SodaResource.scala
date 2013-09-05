package com.socrata.soda.server.resources

import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.HttpService
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.errors.HttpMethodNotAllowed

class SodaResource extends SimpleResource {
  override def methodNotAllowed: HttpService = { req =>
    val allowed = allowedMethods
    Header("Allow", allowed.mkString(",")) ~> SodaUtils.errorResponse(
      req,
      HttpMethodNotAllowed(req.getMethod, allowed))
  }
}

package com.socrata.soda.server

import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.{HttpService, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.soda.server.errors.GeneralNotFoundError
import com.socrata.http.server.routing.SimpleRouteContext._

class SodaRouter(versionService: HttpService)  {
  val router = Routes(
    Route("/version", versionService)
  )

  def route(req: HttpServletRequest): HttpResponse =
    router(req.requestPath.map(InputNormalizer.normalize)) match {
      case Some(s) =>
        s(req)
      case None =>
        SodaUtils.errorResponse(req, GeneralNotFoundError(req.getRequestURI))
    }
}

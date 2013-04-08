package com.socrata.soda.server

import com.socrata.http.routing._
import com.socrata.http.routing.HttpMethods._
import com.socrata.http.server._
import javax.servlet.http._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some

object SodaRouter {

  private val router = new SodaRouter()
  def routedService(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }
}

class SodaRouter extends SimpleRouter[Service[HttpServletRequest, HttpResponse]] (
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"version")   -> SodaFountain.version _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"v")         -> SodaFountain.version _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"resource")  -> SodaFountain.datasetByResourceName _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"resources") -> SodaFountain.datasetByResourceName _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"r")         -> SodaFountain.datasetByResourceName _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"id")        -> SodaFountain.datasetById _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"catalog")   -> SodaFountain.catalog _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"c")         -> SodaFountain.catalog _
)

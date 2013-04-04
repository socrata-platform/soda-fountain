package com.socrata.soda.server

import com.socrata.http.routing._
import com.socrata.http.routing.HttpMethods._
import com.socrata.http.server._
import services.SodaService
import javax.servlet.http._


class SodaRouter extends SimpleRouter[Service[HttpServletRequest, HttpResponse]] (
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"version")   -> SodaService.version _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"resource")  -> SodaService.resource _,
  new SimpleRoute(Set(GET,PUT, POST, DELETE),"catalog")   -> SodaService.catalog _
)

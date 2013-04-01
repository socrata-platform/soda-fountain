package com.socrata.soda.server

import com.socrata.http.routing._
import com.socrata.http.server._
import services.SodaService
import javax.servlet.http._


class SodaRouter extends SimpleRouter[Service[HttpServletRequest, HttpResponse]] (
  new SimpleRoute(Set(HttpMethods.GET),"version") -> SodaService.get _
)

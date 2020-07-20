package com.socrata.soda

import com.socrata.http.server.{HttpService, HttpResponse, Service}
import com.socrata.http.server.routing.IsHttpService

package object server {
  type SodaHttpService = Service[SodaRequest, HttpResponse]

  implicit val isHttpService = new IsHttpService[SodaHttpService] {
    def wrap(s: HttpService): SodaHttpService = { req =>
      s(req.httpRequest)
    }
  }
}

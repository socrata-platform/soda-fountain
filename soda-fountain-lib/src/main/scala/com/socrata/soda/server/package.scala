package com.socrata.soda

import com.socrata.http.server.{HttpResponse, Service}

package object server {
  type SodaHttpService = Service[SodaRequest, HttpResponse]
}

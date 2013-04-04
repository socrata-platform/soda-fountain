package com.socrata.soda.server.services

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

class SodaService {

}

object SodaService {
  def version(request:HttpServletRequest) : HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("Version check not implemented")
  }

  def resource(request:HttpServletRequest): HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def catalog(request:HttpServletRequest): HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("catalog request not implemented")
  }
}

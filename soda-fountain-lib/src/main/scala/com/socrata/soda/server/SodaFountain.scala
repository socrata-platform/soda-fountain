package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.routing.HttpMethods._
import services.DatasetService

class SodaFountain {

}

object SodaFountain {
  def version(request:HttpServletRequest) : HttpServletResponse => Unit = {
    request.getMethod match {
      case GET => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("Version check not implemented")
      case POST | PUT => BadRequest ~> ContentType("text/plain; charset=utf-8") ~> Content("Don't you try to set my version!")
      case DELETE => BadRequest ~> ContentType("text/plain; charset=utf-8") ~> Content("Don't you try to delete my version!")
    }
  }

  def datasetByResourceName(request:HttpServletRequest): HttpServletResponse => Unit = {
    request.getMethod match {
      case GET => DatasetService.getByResourceName
      case POST | PUT => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
      case DELETE => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
  }

  def datasetById(request:HttpServletRequest): HttpServletResponse => Unit = {
    request.getMethod match {
      case GET => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
      case POST | PUT => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
      case DELETE => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
  }

  def catalog(request:HttpServletRequest): HttpServletResponse => Unit = {
    request.getMethod match {
      case GET => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("catalog request not implemented")
      case POST | PUT => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("catalog request not implemented")
      case DELETE => ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("catalog request not implemented")
    }
  }
}

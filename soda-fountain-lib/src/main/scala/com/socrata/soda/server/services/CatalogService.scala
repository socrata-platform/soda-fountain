package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}


trait CatalogService {

  object catalog {
    def get(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }

    def set(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }

    def create()(request:HttpServletRequest): HttpServletResponse => Unit = {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("create request not implemented")
    }

    def delete(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("delete request not implemented")
    }
  }
}




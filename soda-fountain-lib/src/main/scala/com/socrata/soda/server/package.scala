package com.socrata.soda

import scala.language.implicitConversions

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import javax.servlet.http.HttpServletRequest

package object server {

  implicit def toServletHttpRequest(req: HttpRequest): AugmentedHttpServletRequest = req.servletRequest

  def httpRequest(req: HttpServletRequest): HttpRequest = {

    new HttpRequest {

      private val underlying = new HttpRequest.AugmentedHttpServletRequest(req)

      private lazy val scope = new ResourceScope()

      override def servletRequest: AugmentedHttpServletRequest = underlying

      override def resourceScope: ResourceScope = scope
    }
  }

}

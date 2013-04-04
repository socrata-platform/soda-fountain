package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServlet, HttpServletRequest}
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some

class SodaFountain extends HttpServlet {

  val router = new SodaRouter()
  def service(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {service(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {service(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {service(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {service(req)(resp)}
}

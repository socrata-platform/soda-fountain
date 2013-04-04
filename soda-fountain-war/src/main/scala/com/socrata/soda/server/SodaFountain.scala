package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServlet, HttpServletRequest}
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some

class SodaFountain extends HttpServlet {

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {SodaRouter.routedService(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {SodaRouter.routedService(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {SodaRouter.routedService(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {SodaRouter.routedService(req)(resp)}
}

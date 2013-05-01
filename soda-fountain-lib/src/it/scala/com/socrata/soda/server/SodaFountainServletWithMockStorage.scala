package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import com.socrata.soda.server.mocks.{LocalDataCoordinator, MockNameAndSchemaStore}

class SodaFountainServletWithMockStorage extends HttpServlet {

  val fountain = new SodaFountain with MockNameAndSchemaStore with LocalDataCoordinator with SodaRouter

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {fountain.route(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {fountain.route(req)(resp)}

}

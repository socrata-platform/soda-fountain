package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import com.socrata.soda.server.mocks.{LocalDataCoordinator, MockNameAndSchemaStore}
import com.socrata.querycoordinator.client.{LocalQueryCoordinatorClient, QueryCoordinatorClient}
import com.socrata.datacoordinator.client.CuratedDataCoordinatorClient

class SodaFountainServletWithMockStorage extends HttpServlet {

  val fountain = new SodaFountain with MockNameAndSchemaStore with CuratedDataCoordinatorClient with LocalQueryCoordinatorClient with SodaRouter

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {fountain.route(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {fountain.route(req)(resp)}

}

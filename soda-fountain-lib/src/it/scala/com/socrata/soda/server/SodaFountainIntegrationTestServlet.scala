package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import com.socrata.soda.server.mocks.{MockDaos, LocalDataCoordinator, MockNameAndSchemaStore}
import com.socrata.querycoordinator.client.{CuratedQueryCoordinatorClient, LocalQueryCoordinatorClient, QueryCoordinatorClient}
import com.socrata.datacoordinator.client.CuratedDataCoordinatorClient

class SodaFountainIntegrationTestServlet extends HttpServlet {

  val fountain = new SodaFountain with MockNameAndSchemaStore with CuratedDataCoordinatorClient with CuratedQueryCoordinatorClient with SodaRouter with MockDaos

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {fountain.route(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {fountain.route(req)(resp)}

}

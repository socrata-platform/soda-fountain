package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import com.socrata.soda.server.persistence.MockNameAndSchemaStore
import com.socrata.datacoordinator.client.DataCoordinatorClient

class SodaFountainServletWithMockStorage extends HttpServlet {

  val fountain = new SodaFountain(new MockNameAndSchemaStore, new DataCoordinatorClient("http://localhost:12345"))
  val router: SodaRouter = fountain.router

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {router.route(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {router.route(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {router.route(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {router.route(req)(resp)}

}

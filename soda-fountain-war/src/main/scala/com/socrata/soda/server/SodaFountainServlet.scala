package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServlet, HttpServletRequest}
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some
import com.socrata.datacoordinator.client.{CuratorClient, DataCoordinatorClient}
import com.socrata.soda.server.persistence.PostgresStore

class SodaFountainServlet extends HttpServlet {

  val fountain = new SodaFountain with PostgresStore with CuratorClient with SodaRouter
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse)   {fountain.route(req)(resp)}
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse)    {fountain.route(req)(resp)}
  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse) {fountain.route(req)(resp)}
}

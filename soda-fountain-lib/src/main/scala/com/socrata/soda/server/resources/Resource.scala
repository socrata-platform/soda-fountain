package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.RowDAO
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.server.SodaUtils

case class Resource(rowDAO: RowDAO) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  def response(result: RowDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> SodaUtils.JsonContent(value)
    }
  }

  def query(resourceName: ResourceName)(req: HttpServletRequest): HttpResponse =
    response(rowDAO.query(resourceName, Option(req.getParameter("$query")).getOrElse("select *")))

  case class service(resourceName: ResourceName) extends SodaResource {
    override def get = query(resourceName)
  }

  case class rowService(resourceName: ResourceName, rowId: String) extends SodaResource
}

package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.RowDAO
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.wiremodels.InputUtils
import com.rojoma.simplearm.util._
import com.rojoma.json.io.{CompactJsonWriter, EventTokenIterator}

case class Resource(rowDAO: RowDAO, maxRowSize: Long) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  def response(result: RowDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> SodaUtils.JsonContent(value)
    }
  }

  def upsertResponse(response: HttpServletResponse)(result: RowDAO.UpsertResult) {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.StreamSuccess(report) =>
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType(SodaUtils.jsonContentTypeUtf8) // TODO: negotiate charset too
        using(response.getWriter) { w =>
          // TODO: send actual response
          val jw = new CompactJsonWriter(w)
          w.write('[')
          if(report.nonEmpty) jw.write(report.next())
          while(report.nonEmpty) {
            w.write(',')
            jw.write(report.next())
          }
          w.write("]\n")
        }
    }
  }

  def query(resourceName: ResourceName)(req: HttpServletRequest): HttpResponse =
    response(rowDAO.query(resourceName, Option(req.getParameter("$query")).getOrElse("select *")))

  def upsert(resourceName: ResourceName)(req: HttpServletRequest)(response: HttpServletResponse) {
    InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
      case Right(boundedIt) =>
        rowDAO.upsert(resourceName, boundedIt)(upsertResponse(response))
      case Left(err) =>
        SodaUtils.errorResponse(req, err, resourceName)(response)
    }
  }

  def replace(resourceName: ResourceName)(req: HttpServletRequest)(response: HttpServletResponse) {
    InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
      case Right(boundedIt) =>
        rowDAO.replace(resourceName, boundedIt)(upsertResponse(response))
      case Left(err) =>
        SodaUtils.errorResponse(req, err, resourceName)(response)
    }
  }

  case class service(resourceName: ResourceName) extends SodaResource {
    override def get = query(resourceName)
    override def post = upsert(resourceName)
    override def put = replace(resourceName)
  }

  case class rowService(resourceName: ResourceName, rowId: String) extends SodaResource
}

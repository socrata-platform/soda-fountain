package com.socrata.soda.server.resources

import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.RowDAO
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.wiremodels.InputUtils
import com.rojoma.simplearm.util._
import com.rojoma.json.io.{CompactJsonWriter, EventTokenIterator}
import com.rojoma.json.ast.{JString, JArray}
import com.socrata.soda.server.errors.RowNotFound

case class Resource(rowDAO: RowDAO, maxRowSize: Long) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  def response(result: RowDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> SodaUtils.JsonContent(value)
    }
  }

  def rowResponse(req: HttpServletRequest, result: RowDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> SodaUtils.JsonContent(value)
      case RowDAO.RowNotFound(value) =>
        SodaUtils.errorResponse(req, RowNotFound(value))
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

  case class service(resourceName: ResourceName) extends SodaResource {
    override def get = { req =>
      response(rowDAO.query(resourceName, Option(req.getParameter("$query")).getOrElse("select *")))
    }

    override def post = { req => response =>
      InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
        case Right(boundedIt) =>
          rowDAO.upsert(user(req), resourceName, boundedIt)(upsertResponse(response))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName)(response)
      }
    }

    override def put = { req => response =>
      InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
        case Right(boundedIt) =>
          rowDAO.replace(user(req), resourceName, boundedIt)(upsertResponse(response))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName)(response)
      }
    }
  }

  case class rowService(resourceName: ResourceName, rowId: RowSpecifier) extends SodaResource {
    override def get = rowResponse(_, rowDAO.getRow(resourceName, rowId))

    override def post = { req => response =>
      InputUtils.jsonSingleObjectStream(req, maxRowSize) match {
        case Right(rowJVal) =>
          rowDAO.upsert(user(req), resourceName, Iterator.single(rowJVal))(upsertResponse(response))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName)(response)
      }
    }

    override def delete = { req => response =>
      rowDAO.deleteRow(user(req), resourceName, rowId)(upsertResponse(response))
    }
  }
}

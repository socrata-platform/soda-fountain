package com.socrata.soda.server.resources

import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.RowDAO
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.wiremodels.InputUtils
import com.socrata.soda.server.errors._
import com.socrata.http.common.util.ContentNegotiation
import com.socrata.soda.server.export.Exporter
import com.rojoma.simplearm.util._
import com.rojoma.json.io.CompactJsonWriter
import java.nio.charset.StandardCharsets
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import java.security.MessageDigest
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.http.server.util.{Precondition, EntityTag}
import scala.Some
import com.socrata.soda.server.errors.DatasetNotFound
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.soda.server.errors.RowNotFound
import com.socrata.soda.server.id.RowSpecifier
import com.socrata.soda.server.highlevel.RowDAO.MaltypedData
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{OtherReportItem, UpsertReportItem}

case class Resource(rowDAO: RowDAO, etagObfuscator: ETagObfuscator, maxRowSize: Long) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  val headerHashAlg = "SHA1"
  val headerHashLength = MessageDigest.getInstance(headerHashAlg).getDigestLength
  def headerHash(req: HttpServletRequest) = {
    val hash = MessageDigest.getInstance(headerHashAlg)
    hash.update(Option(req.getQueryString).toString.getBytes(StandardCharsets.UTF_8))
    hash.update(255.toByte)
    for(field <- ContentNegotiation.headers) {
      hash.update(field.getBytes(StandardCharsets.UTF_8))
      hash.update(254.toByte)
      for(elem <- req.headers(field)) {
        hash.update(elem.getBytes(StandardCharsets.UTF_8))
        hash.update(254.toByte)
      }
      hash.update(255.toByte)
    }
    hash.digest()
  }

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

  def upsertResponse(request: HttpServletRequest, response: HttpServletResponse)(result: RowDAO.UpsertResult) {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.StreamSuccess(report) =>
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType(SodaUtils.jsonContentTypeUtf8) // TODO: negotiate charset too
        using(response.getWriter) { w =>
          // TODO: send actual response
          val jw = new CompactJsonWriter(w)
          w.write('[')
          var wroteOne = false
          while(report.hasNext) {
            report.next() match {
              case UpsertReportItem(items) =>
                while(items.hasNext) {
                  if(wroteOne) w.write(',')
                  else wroteOne = true
                  jw.write(items.next())
                }
              case OtherReportItem => // nothing; probably shouldn't have occurred!
            }
          }
          w.write("]\n")
        }
      case mismatch : MaltypedData =>
        SodaUtils.errorResponse(request, new ColumnSpecMaltyped(mismatch.column.name, mismatch.expected.name.name, mismatch.got))(response)
    }
  }

  implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

  def extensions(s: String) = Exporter.exporterExtensions.contains(Exporter.canonicalizeExtension(s))

  case class service(resourceName: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = { req: HttpServletRequest => response: HttpServletResponse =>
      val qpQuery = "$query" // Query parameter row count
      val qpRowCount = "$$row_count" // Query parameter row count
      val qpSecondary = "$$store"
      val suffix = headerHash(req)
      val precondition = req.precondition.map(etagObfuscator.deobfuscate)
      def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
      precondition.filter(_.endsWith(suffix)) match {
        case Right(newPrecondition) =>
          req.negotiateContent match {
            case Some((mimeType, charset, language)) =>
              val exporter = Exporter.exportForMimeType(mimeType)
              rowDAO.query(resourceName.value, newPrecondition.map(_.dropRight(suffix.length)), Option(req.getParameter(qpQuery)).getOrElse("select *"), Option(req.getParameter(qpRowCount)), Option(req.getParameter(qpSecondary))) match {
                case RowDAO.QuerySuccess(code, etags, schema, rows) =>
                  response.setStatus(HttpServletResponse.SC_OK)
                  response.setContentType(mimeType.toString)
                  response.setHeader("Vary", ContentNegotiation.headers.mkString(","))
                  ETags(etags.map(prepareTag))(response)
                  exporter.export(response, charset, schema, rows)
                case RowDAO.DatasetNotFound(resourceName) =>
                  SodaUtils.errorResponse(req, DatasetNotFound(resourceName))(response)
              }
            case None =>
              // TODO better error
              NotAcceptable(response)
          }
        case Left(Precondition.FailedBecauseNoMatch) =>
          SodaUtils.errorResponse(req, EtagPreconditionFailed)(response)
      }
    }

    override def post = { req => response =>
      InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
        case Right(boundedIt) =>
          rowDAO.upsert(user(req), resourceName.value, boundedIt)(upsertResponse(req, response))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName.value)(response)
      }
    }

    override def put = { req => response =>
      InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
        case Right(boundedIt) =>
          rowDAO.replace(user(req), resourceName.value, boundedIt)(upsertResponse(req, response))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName.value)(response)
      }
    }
  }

  case class rowService(resourceName: ResourceName, rowId: RowSpecifier) extends SodaResource {

    implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

    override def get = { req: HttpServletRequest => response: HttpServletResponse =>
      val suffix = headerHash(req)
      val qpSecondary = "$$store"
      val precondition = req.precondition.map(etagObfuscator.deobfuscate)
      def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
      precondition.filter(_.endsWith(suffix)) match {
        case Right(newPrecondition) =>
          // not using req.negotiateContent because we can't assume `.' signifies an extension
          contentNegotiation(req.accept, req.contentType, None, req.acceptCharset, req.acceptLanguage) match {
            case Some((mimeType, charset, language)) =>
              val exporter = Exporter.exportForMimeType(mimeType)
              rowDAO.getRow(resourceName, newPrecondition.map(_.dropRight(suffix.length)), rowId, Option(req.getParameter(qpSecondary))) match {
                case RowDAO.SingleRowQuerySuccess(code, etags, schema, row) =>
                  response.setStatus(HttpServletResponse.SC_OK)
                  response.setContentType(mimeType.toString)
                  response.setHeader("Vary", ContentNegotiation.headers.mkString(","))
                  ETags(etags.map(prepareTag))(response)
                  exporter.export(response, charset, schema, Iterator.single(row), singleRow = true)
                case RowDAO.RowNotFound(row) =>
                  SodaUtils.errorResponse(req, RowNotFound(row))(response)
                case RowDAO.DatasetNotFound(resourceName) =>
                  SodaUtils.errorResponse(req, DatasetNotFound(resourceName))(response)
                case RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
                  SodaUtils.errorResponse(req, ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))(response)
                case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
                  SodaUtils.errorResponse(req, EtagPreconditionFailed)(response)
              }
            case None =>
              // TODO better error
              NotAcceptable(response)
          }
        case Left(Precondition.FailedBecauseNoMatch) =>
          SodaUtils.errorResponse(req, EtagPreconditionFailed)(response)
      }
    }

    override def post = { req => response =>
      InputUtils.jsonSingleObjectStream(req, maxRowSize) match {
        case Right(rowJVal) =>
          rowDAO.upsert(user(req), resourceName, Iterator.single(rowJVal))(upsertResponse(req, response))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName)(response)
      }
    }

    override def delete = { req => response =>
      rowDAO.deleteRow(user(req), resourceName, rowId)(upsertResponse(req, response))
    }
  }
}

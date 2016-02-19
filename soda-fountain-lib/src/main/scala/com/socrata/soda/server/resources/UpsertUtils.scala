package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.{JObject, JNumber, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.http.server.HttpRequest
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{OtherReportItem, UpsertReportItem, ReportItem}
import com.socrata.soda.server.{errors => SodaErrors, _}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soda.server.highlevel.{ExportParam, RowDataTranslator, RowDAO}
import com.socrata.soda.server.highlevel.RowDAO.MaltypedData
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import scala.language.existentials

object UpsertUtils {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def handleUpsertErrors(req: HttpServletRequest, response: HttpServletResponse)
                        (successHandler: (HttpServletResponse, Iterator[ReportItem]) => Unit)
                        (upsertResult: RowDAO.UpsertResult) = {
    import RowDataTranslator._

    val handleResponse = upsertResponse(req, response, _: RowDAO.UpsertResult)(successHandler)

    try {
      handleResponse(upsertResult)
    } catch {
      case MaltypedDataEx(columnName, expected, got) =>
        handleResponse(RowDAO.MaltypedData(columnName, expected, got))
      case UnknownColumnEx(columnName)               =>
        handleResponse(RowDAO.UnknownColumn(columnName))
      case DeleteNoPKEx                              =>
        handleResponse(RowDAO.CannotDeletePrimaryKey)
      case NotAnObjectOrSingleElementArrayEx(obj)    =>
        handleResponse(RowDAO.RowNotAnObject(obj))
      case ComputationHandlerNotFoundEx(typ)         =>
        handleResponse(RowDAO.ComputationHandlerNotFound(typ))
    }
  }

  private def upsertResponse(request: HttpServletRequest, response: HttpServletResponse, result: RowDAO.UpsertResult)
                    (successHandler: (HttpServletResponse, Iterator[ReportItem]) => Unit) {
    // TODO: Negotiate content-type
    result match {
      case RowDAO.StreamSuccess(report) =>
        successHandler(response, report)
      case mismatch : MaltypedData =>
        SodaUtils.errorResponse(request, new SodaErrors.ColumnSpecMaltyped(mismatch.column.name, mismatch.expected.name.name, mismatch.got))(response)
      case RowDAO.RowNotFound(rowSpecifier) =>
        SodaUtils.errorResponse(request, SodaErrors.RowNotFound(rowSpecifier))(response)
      case RowDAO.RowPrimaryKeyIsNonexistentOrNull(rowSpecifier) =>
        SodaUtils.errorResponse(request, SodaErrors.RowPrimaryKeyNonexistentOrNull(rowSpecifier))(response)
      case RowDAO.UnknownColumn(columnName) =>
        SodaUtils.errorResponse(request, SodaErrors.RowColumnNotFound(columnName))(response)
      case RowDAO.ComputationHandlerNotFound(typ) =>
        SodaUtils.errorResponse(request, SodaErrors.ComputationHandlerNotFound(typ))(response)
      case RowDAO.CannotDeletePrimaryKey =>
        SodaUtils.errorResponse(request, SodaErrors.CannotDeletePrimaryKey)(response)
      case RowDAO.RowNotAnObject(obj) =>
        SodaUtils.errorResponse(request, SodaErrors.UpsertRowNotAnObject(obj))(response)
      case RowDAO.DatasetNotFound(dataset) =>
        SodaUtils.errorResponse(request, SodaErrors.DatasetNotFound(dataset))(response)
      case RowDAO.SchemaOutOfSync =>
        SodaUtils.errorResponse(request, SodaErrors.SchemaInvalidForMimeType)(response)
      case RowDAO.InvalidRequest(client, status, body) =>
        SodaUtils.errorResponse(request, SodaErrors.InternalError(s"Error from $client:", "code"  -> JNumber(status),
          "data" -> body))(response)
      case RowDAO.QCError(status, qcErr) =>
        SodaUtils.errorResponse(request, SodaErrors.ErrorReportedByQueryCoordinator(status, qcErr))(response)
      case RowDAO.InternalServerError(status, client, code, tag, data) =>
        SodaUtils.errorResponse(request, SodaErrors.InternalError(s"Error from $client:",
          "status" -> JNumber(status),
          "code"  -> JString(code),
          "data" -> JString(data),
          "tag"->JString(tag)))(response)
    }
  }

  def writeUpsertResponse(response: HttpServletResponse, report: Iterator[ReportItem]) = {
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
  }

  def writeSingleRowUpsertResponse(resourceName: ResourceName, export: Export, req: HttpRequest)
                                  (response: HttpServletResponse, report: Iterator[ReportItem]): Unit = {
    var wroteOne = false
    while(report.hasNext) {
      report.next() match {
        case UpsertReportItem(items) =>
          while(items.hasNext) {
            val item = items.next()
            if (wroteOne) {
              // It is too late to alter response.  Just log an error.
              log.error("single row upsert error, too many report-item-id {}", item)
            } else {
              wroteOne = true
              item match {
                case JObject(rowInfo) =>
                  rowInfo("id") match {
                    case JString(rowId) =>
                      val param = ExportParam(None, None, Seq.empty[ColumnRecordLike], None,
                                              sorted = false, rowId = Some(rowId))
                      export.exportCopy(resourceName,
                                        "latest",
                                        Some("json"),
                                        excludeSystemFields = false,
                                        param,
                                        true)(req)(response)
                    case unknown =>
                      log.error("single row upsert error, malformed report-item-id {}", unknown)
                      SodaUtils.errorResponse(req,
                        SodaErrors.InternalError("upsert-error-malformed-report-item-id"))(response)
                  }
                case unknown =>
                  log.error("single row upsert error, malformed report-item {}", unknown)
                  SodaUtils.errorResponse(req,
                    SodaErrors.InternalError("upsert-error-malformed-report-item"))(response)
              }
            }
          }
        case OtherReportItem => // nothing; probably shouldn't have occurred!
          log.error("single row upsert error, got other-report-item")
          SodaUtils.errorResponse(req,
            SodaErrors.InternalError("upsert-error-malformed-other-report-item"))(response)
      }
    }
  }
}

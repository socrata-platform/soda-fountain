package com.socrata.soda.server.resources

import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{OtherReportItem, UpsertReportItem}
import com.socrata.soda.server.{errors => SodaErrors, SodaUtils}
import com.socrata.soda.server.highlevel.{RowDataTranslator, RowDAO}
import com.socrata.soda.server.highlevel.RowDAO.MaltypedData
import com.socrata.soda.server.id.ResourceName
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

object UpsertUtils {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def handleUpsertErrors(req: HttpServletRequest,
                         response: HttpServletResponse,
                         resourceName: ResourceName)(upsertResult: RowDAO.UpsertResult) = {
    import RowDataTranslator._

    try {
      upsertResponse(req, response)(upsertResult)
    } catch {
      case MaltypedDataEx(columnName, expected, got) =>
        upsertResponse(req, response)(RowDAO.MaltypedData(columnName, expected, got))
      case UnknownColumnEx(columnName)               =>
        upsertResponse(req, response)(RowDAO.UnknownColumn(columnName))
      case DeleteNoPKEx                              =>
        upsertResponse(req, response)(RowDAO.DeleteWithoutPrimaryKey)
      case NotAnObjectOrSingleElementArrayEx(obj)    =>
        upsertResponse(req, response)(RowDAO.RowNotAnObject(obj))
      case ComputedColumnNotWritableEx(columnName)   =>
        upsertResponse(req, response)(RowDAO.ComputedColumnNotWritable(columnName))
      case ComputationHandlerNotFoundEx(typ)         =>
        upsertResponse(req, response)(RowDAO.ComputationHandlerNotFound(typ))
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
        SodaUtils.errorResponse(request, new SodaErrors.ColumnSpecMaltyped(mismatch.column.name, mismatch.expected.name.name, mismatch.got))(response)
      case RowDAO.RowNotFound(rowSpecifier) =>
        SodaUtils.errorResponse(request, SodaErrors.RowNotFound(rowSpecifier))(response)
      case RowDAO.UnknownColumn(columnName) =>
        SodaUtils.errorResponse(request, SodaErrors.RowColumnNotFound(columnName))(response)
      case RowDAO.ComputationHandlerNotFound(typ) =>
        SodaUtils.errorResponse(request, SodaErrors.ComputationHandlerNotFound(typ))(response)
      case RowDAO.ComputedColumnNotWritable(columnName) =>
        SodaUtils.errorResponse(request, SodaErrors.ComputedColumnNotWritable(columnName))(response)
    }
  }
}

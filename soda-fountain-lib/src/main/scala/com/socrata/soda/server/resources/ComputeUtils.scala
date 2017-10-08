package com.socrata.soda.server.resources

import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpRequest
import com.socrata.soda.server.responses.GeneralNotFoundError
import com.socrata.soda.server.{responses => SodaError, _}
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletResponse

class ComputeUtils(columnDAO: ColumnDAO) {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[ComputeUtils])

  def compute(req: HttpRequest,
              response: HttpServletResponse,
              resourceScope: ResourceScope,
              resourceName: ResourceName,
              columnName: ColumnName,
              user: String)
             (successHandler: HttpServletResponse => Unit): Unit = {
    columnDAO.getColumn(resourceName, columnName) match {
      case ColumnDAO.Found(dataset, column, _)         =>
        column.computationStrategy match {
          case Some(strategy) => successHandler(response)
          case None           => SodaUtils.response(req, SodaError.NotAComputedColumn(columnName))(response)
        }
      case ColumnDAO.DatasetNotFound(dataset) =>
        SodaUtils.response(req, SodaError.DatasetNotFound(resourceName))(response)
      case ColumnDAO.ColumnNotFound(column)   =>
        SodaUtils.response(req, SodaError.ColumnNotFound(resourceName, columnName))(response)
      case _@x =>
        log.warn("case is NOT implemented")
        SodaUtils.response(req, GeneralNotFoundError("unexpected match case"))
    }
  }

  def writeComputeResponse(resourceName: ResourceName,
                           columnName: ColumnName,
                           responseCode: Int,
                           response: HttpServletResponse) {
    response.setStatus(responseCode)
    response.setContentType(SodaUtils.jsonContentTypeUtf8) // TODO: negotiate charset too
    using(response.getWriter) { w =>
      val jw = new CompactJsonWriter(w)
      // TODO use .ast.v3 instead
      jw.write(com.rojoma.json.ast.JObject(Map("resource_name" -> com.rojoma.json.ast.JString(resourceName.name),
                           "column_name"   -> com.rojoma.json.ast.JString(columnName.name),
                           "rows_computed" -> com.rojoma.json.ast.JNumber(0))))
      w.write("\n")
    }
  }
}

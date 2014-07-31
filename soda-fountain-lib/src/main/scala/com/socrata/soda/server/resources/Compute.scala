package com.socrata.soda.server.resources

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.export.JsonExporter
import com.socrata.soda.server.highlevel.{ExportDAO, RowDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletRequest

case class Compute(exportDAO: ExportDAO, rowDAO: RowDAO, etagObfuscator: ETagObfuscator) {
  sealed trait ComputeResult
  case object Success extends ComputeResult
  case object Failed extends ComputeResult

  def response(request: HttpServletRequest, result: ComputeResult): HttpResponse = {
    result match {
      case Success => OK ~> SodaUtils.JsonContent("TODO")
      case Failed => InternalServerError ~> SodaUtils.JsonContent("TODO")
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req =>
      response(req, Success)
    }
  }
}

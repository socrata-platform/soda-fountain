package com.socrata.soda.server.resources

import com.rojoma.json.ast._
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.http.server.util.NoPrecondition
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.{errors => SodaError}
import com.socrata.soda.server.export.JsonExporter
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

class ComputeUtils(columnDAO: ColumnDAO, exportDAO: ExportDAO, rowDAO: RowDAO, computedColumns: ComputedColumnsLike) {

  private def columnsToExport(dataset: DatasetRecordLike, computationStrategy: ComputationStrategyRecord): Seq[ColumnRecordLike] = {
    def sourceColumns    = getSourceColumns(dataset, computationStrategy)
    def primaryKeyColumn = dataset.columnsById(dataset.primaryKey)
    sourceColumns ++ Seq(primaryKeyColumn)
  }

  private def getSourceColumns(dataset: DatasetRecordLike, computationStrategy: ComputationStrategyRecord): Seq[ColumnRecordLike] = {
    computationStrategy.sourceColumns match {
      case Some(columnIds: Seq[String]) =>
        val trans = new RowDataTranslator(dataset, false)
        trans.getInfoForColumnList(columnIds)
      case None => Seq()
    }
  }

  def compute(req: HttpServletRequest,
              response: HttpServletResponse,
              resourceName: ResourceName,
              columnName: ColumnName,
              user: String)(handleResponse: RowDAO.UpsertResult => Unit): Unit = {
    columnDAO.getColumn(resourceName, columnName) match {
      case ColumnDAO.Found(dataset, column, _)         =>
        column.computationStrategy match {
          case Some(strategy) => compute(req, response, dataset, column, user)(handleResponse)
          case None           => SodaUtils.errorResponse(req, SodaError.NotAComputedColumn(columnName))(response)
        }
      case ColumnDAO.DatasetNotFound(dataset) =>
        SodaUtils.errorResponse(req, SodaError.DatasetNotFound(resourceName))(response)
      case ColumnDAO.ColumnNotFound(column)   =>
        SodaUtils.errorResponse(req, SodaError.ColumnNotFound(resourceName, columnName))(response)
    }
  }

  def compute(req: HttpServletRequest,
              response: HttpServletResponse,
              dataset: DatasetRecordLike,
              column: ColumnRecordLike,
              user: String)(handleResponse: RowDAO.UpsertResult => Unit): Unit =
    column.computationStrategy match {
      case Some(strategy) =>
        val columns = columnsToExport(dataset, strategy)
        exportDAO.export(dataset.resourceName,
          JsonExporter.validForSchema,
          columns,
          NoPrecondition,
          None,
          None,
          None,
          "latest",
          sorted = false) {
          case ExportDAO.Success(schema, newTag, rows) =>
            val transformer = new RowDataTranslator(dataset, false)
            val upsertRows = transformer.transformDcRowsForUpsert(computedColumns, Seq(column), schema, rows)
            rowDAO.upsert(user, dataset, upsertRows)(handleResponse)
          case ExportDAO.PreconditionFailed => SodaUtils.errorResponse(req, SodaError.EtagPreconditionFailed)(response)
          case ExportDAO.NotModified(etags) => SodaUtils.errorResponse(req, SodaError.ResourceNotModified(Nil, None))(response)
          case ExportDAO.NotFound => SodaUtils.errorResponse(req, SodaError.DatasetNotFound(dataset.resourceName))(response)
          case ExportDAO.SchemaInvalidForMimeType => SodaUtils.errorResponse(req, SodaError.SchemaInvalidForMimeType)(response)
        }
      case None => SodaUtils.errorResponse(req, SodaError.NotAComputedColumn(column.fieldName))(response)
    }

  def writeComputeResponse(resourceName: ResourceName,
                           columnName: ColumnName,
                           responseCode: Int,
                           response: HttpServletResponse,
                           report: Iterator[ReportItem]) {
    response.setStatus(responseCode)
    response.setContentType(SodaUtils.jsonContentTypeUtf8) // TODO: negotiate charset too
    using(response.getWriter) { w =>
      val jw = new CompactJsonWriter(w)
      var rowsComputed = 0
      while (report.hasNext) {
        report.next() match {
          case UpsertReportItem(items) =>
            // Data coordinator client throws an exception if we don't
            // iterate through the results.
            while(items.hasNext) {
              rowsComputed = rowsComputed + 1
              items.next()
            }
          case OtherReportItem => // nothing; probably shouldn't have occurred!
        }
      }
      jw.write(JObject(Map("resource_name" -> JString(resourceName.name),
                           "column_name"   -> JString(columnName.name),
                           "rows_computed" -> JNumber(rowsComputed))))
      w.write("\n")
    }
  }
}

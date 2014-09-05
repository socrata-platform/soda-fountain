package com.socrata.soda.server.resources

import com.socrata.http.server.util.NoPrecondition
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.{errors => SodaError}
import com.socrata.soda.server.export.JsonExporter
import com.socrata.soda.server.highlevel.{RowDAO, RowDataTranslator, ExportDAO}
import com.socrata.soda.server.persistence._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

class ComputeUtils(exportDAO: ExportDAO, rowDAO: RowDAO, computedColumns: ComputedColumnsLike) {

  def columnsToExport(dataset: DatasetRecordLike, computationStrategy: ComputationStrategyRecord): Seq[ColumnRecordLike] = {
    def sourceColumns    = getSourceColumns(dataset, computationStrategy)
    def primaryKeyColumn = dataset.columnsById(dataset.primaryKey)
    sourceColumns ++ Seq(primaryKeyColumn)
  }

  def getSourceColumns(dataset: DatasetRecordLike, computationStrategy: ComputationStrategyRecord): Seq[ColumnRecordLike] = {
    computationStrategy.sourceColumns match {
      case Some(columnIds: Seq[String]) =>
        val trans = new RowDataTranslator(dataset, false)
        trans.getInfoForColumnList(columnIds)
      case None => Seq()
    }
  }

  def compute(req: HttpServletRequest,
              response: HttpServletResponse,
              dataset: MinimalDatasetRecord,
              column: ColumnRecordLike,
              user: String) =
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
            rowDAO.upsert(user, dataset, upsertRows)(UpsertUtils.handleUpsertErrors(req, response, dataset.resourceName))
          case ExportDAO.PreconditionFailed => SodaUtils.errorResponse(req, SodaError.EtagPreconditionFailed)(response)
          case ExportDAO.NotModified(etags) => SodaUtils.errorResponse(req, SodaError.ResourceNotModified(Nil, None))(response)
          case ExportDAO.NotFound => SodaUtils.errorResponse(req, SodaError.DatasetNotFound(dataset.resourceName))(response)
          case ExportDAO.SchemaInvalidForMimeType => SodaUtils.errorResponse(req, SodaError.SchemaInvalidForMimeType)(response)
        }
      case None => SodaUtils.errorResponse(req, SodaError.NotAComputedColumn(column.fieldName))(response)
    }
}

package com.socrata.soda.server.resources

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.NoPrecondition
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.{errors => SodaError}
import com.socrata.soda.server.export.JsonExporter
import com.socrata.soda.server.highlevel.{RowDataTranslator, ExportDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletRequest

case class Compute(store: NameAndSchemaStore, exportDAO: ExportDAO, etagObfuscator: ETagObfuscator) {
  sealed trait ComputeResult
  case class Success(seq: Seq[String]) extends ComputeResult
  case object Failed extends ComputeResult

  def response(request: HttpServletRequest, result: ComputeResult): HttpResponse = {
    result match {
      case Success(seq) => OK ~> SodaUtils.JsonContent(seq)
      case Failed => InternalServerError ~> SodaUtils.JsonContent("TODO")
    }
  }

  def columnsToExport(dataset: DatasetRecordLike, computationStrategy: ComputationStrategyRecord): Seq[ColumnRecordLike] = {
    def sourceColumns    = getSourceColumns(dataset, computationStrategy)
    def primaryKeyColumn = dataset.columnsById(dataset.primaryKey)
    sourceColumns ++ Seq(primaryKeyColumn)
  }

  def getSourceColumns(dataset: DatasetRecordLike, computationStrategy: ComputationStrategyRecord): Seq[ColumnRecordLike] = {
    computationStrategy.sourceColumns match {
      case Some(columnNames: Seq[String]) =>
        val trans = new RowDataTranslator(dataset, false)
        trans.getInfoForColumnList(columnNames)
      case None => Seq()
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req =>
      store.lookupDataset(resourceName) match {
        case Some(dataset) =>
          dataset.columns.find(_.fieldName.name.equals(columnName.name)) match {
            case Some(column) =>
              column.computationStrategy match {
                case Some(strategy) =>
                  val columns = columnsToExport(dataset, strategy)
                  // TODO : Is it actually the published copy we want to compute on, or a different copy?
                  exportDAO.export(resourceName, JsonExporter.validForSchema, columns, NoPrecondition, None, None, None, "published", sorted = false) {
                    case ExportDAO.Success(schema, newTag, rows) =>
                      // TODO : Do the compute and upsert
                      response(req, Success(columns.map(_.id.underlying)))
                    case ExportDAO.PreconditionFailed            => SodaUtils.errorResponse(req, SodaError.EtagPreconditionFailed)
                    case ExportDAO.NotModified(etags)            => SodaUtils.errorResponse(req, SodaError.ResourceNotModified(Nil, None))
                    case ExportDAO.NotFound                      => SodaUtils.errorResponse(req, SodaError.DatasetNotFound(resourceName))
                    case ExportDAO.SchemaInvalidForMimeType      => SodaUtils.errorResponse(req, SodaError.SchemaInvalidForMimeType)
                  }
                case None => SodaUtils.errorResponse(req, SodaError.NotAComputedColumn(columnName))
              }
            case None => SodaUtils.errorResponse(req, SodaError.ColumnNotFound(resourceName, columnName))
          }
        case None => SodaUtils.errorResponse(req, SodaError.DatasetNotFound(resourceName))
      }
    }
  }
}

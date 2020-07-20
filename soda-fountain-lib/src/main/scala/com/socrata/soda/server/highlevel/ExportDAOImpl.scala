package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.v2._
import com.socrata.http.server.util.{Precondition, RequestId}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord, ColumnRecordLike, NameAndSchemaStore}
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.SodaUtils.traceHeaders
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import scala.util.control.ControlThrowable
import com.socrata.soda.server.util.schema.SchemaHash
import com.socrata.soda.server.copy.Stage

class ExportDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient) extends ExportDAO {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[ExportDAOImpl])

  def lookupDataset(resourceName: ResourceName, copy: Option[Stage]): Option[DatasetRecord] = {
    store.lookupDataset(resourceName, copy)
  }

  private def convertStrategy(strat: JValue, idNameMap: Map[ColumnId, ColumnName]): JValue =
    strat match {
      case JString(s) =>
        idNameMap.get(ColumnId(s)) match {
          case None => JString(s)
          case Some(n) => JString(n.name)
        }
      case otherAtom: JAtom => otherAtom
      case JArray(arr) => JArray(arr.map(convertStrategy(_, idNameMap)))
      case JObject(fields) => JObject(fields.mapValues(convertStrategy(_, idNameMap)))
    }

  def export[T](dataset: ResourceName,
                precondition: Precondition,
                copy: String,
                param: ExportParam,
                requestId: RequestId.RequestId,
                resourceScope: ResourceScope): ExportDAO.Result =
    retryable(limit = 5) {
      lookupDataset(dataset, Stage(copy)) match {
        case Some(ds) =>
          val schemaHash = param.onlyColumns match {
            case Seq() => ds.schemaHash
            case _     =>
              SchemaHash.computeHash(ds.locale, ds.primaryKey, param.onlyColumns.map { col => (col.id, col.typ) })
          }
          val dcColumnIds = param.onlyColumns.map(_.id.underlying)
          dc.export(ds.handle, schemaHash, dcColumnIds, precondition,
                    param.ifModifiedSince,
                    param.limit,
                    param.offset,
                    copy,
                    sorted = param.sorted,
                    param.rowId,
                    resourceScope) match {
            case DataCoordinatorClient.ExportResult(jvalues, _, etag) =>
              val decodedSchema = CJson.decode(jvalues, JsonColumnRep.forDataCoordinatorType)
              val schema = decodedSchema.schema
              val simpleSchema = ExportDAO.CSchema(
                schema.approximateRowCount,
                schema.dataVersion,
                schema.lastModified.map(time => ExportDAO.dateTimeParser.parseDateTime(time)),
                schema.locale,
                schema.pk.map(ds.columnsById(_).fieldName),
                schema.rowCount,
                schema.schema.map { f =>
                  ColumnInfo(ds.columnsById(f.columnId).id,
                             ds.columnsById(f.columnId).fieldName,
                             f.typ,
                             f.computationStrategy.map(convertStrategy(_, ds.columnsById.mapValues(_.fieldName))))
                }
              )
              ExportDAO.Success(simpleSchema, etag, decodedSchema.rows)
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(ds.systemId, newSchema)
              retry()
            case DataCoordinatorClient.NotModifiedResult(etags) =>
              ExportDAO.NotModified(etags)
            case DataCoordinatorClient.PreconditionFailedResult =>
              ExportDAO.PreconditionFailed
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              ExportDAO.NotFound(dataset)
            case DataCoordinatorClient.InvalidRowIdResult =>
              ExportDAO.InvalidRowId
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, msg) =>
              ExportDAO.InternalServerError(code, tag, msg)
            case x =>
              ExportDAO.InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          ExportDAO.NotFound(dataset)
      }
    }

  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

}

package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.{Precondition, RequestId}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord, ColumnRecordLike, NameAndSchemaStore}
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.SodaUtils.traceHeaders
import com.socrata.soda.server.wiremodels.JsonColumnRep
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

  def export[T](dataset: ResourceName,
                schemaCheck: Seq[ColumnRecordLike] => Boolean,
                precondition: Precondition,
                copy: String,
                param: ExportParam,
                requestId: RequestId.RequestId)(f: ExportDAO.Result => T): T =
    retryable(limit = 5) {
      lookupDataset(dataset, Stage(copy)) match {
        case Some(ds) =>
          if (schemaCheck(ds.columns)) {
            val schemaHash = param.onlyColumns match {
              case Seq() => ds.schemaHash
              case _     =>
                SchemaHash.computeHash(ds.locale, ds.primaryKey, param.onlyColumns.map { col => (col.id, col.typ) })
            }
            val dcColumnIds = param.onlyColumns.map(_.id.underlying)
            dc.export(ds.systemId, schemaHash, dcColumnIds, precondition,
                      param.ifModifiedSince,
                      param.limit,
                      param.offset,
                      copy,
                      sorted = param.sorted,
                      param.rowId,
                      extraHeaders = traceHeaders(requestId, dataset)) {
              case DataCoordinatorClient.ExportResult(jvalues, etag) =>
                val decodedSchema = CJson.decode(jvalues, JsonColumnRep.forDataCoordinatorType)
                val schema = decodedSchema.schema
                val simpleSchema = ExportDAO.CSchema(
                  schema.approximateRowCount,
                  schema.dataVersion,
                  schema.lastModified.map(time => dateTimeParser.parseDateTime(time)),
                  schema.locale,
                  schema.pk.map(ds.columnsById(_).fieldName),
                  schema.rowCount,
                  schema.schema.map {
                    f => ColumnInfo(ds.columnsById(f.c).id, ds.columnsById(f.c).fieldName, ds.columnsById(f.c).name, f.t)
                  }
                )
                f(ExportDAO.Success(simpleSchema, etag, decodedSchema.rows))
              case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
                store.resolveSchemaInconsistency(ds.systemId, newSchema)
                retry()
              case DataCoordinatorClient.NotModifiedResult(etags) =>
                f(ExportDAO.NotModified(etags))
              case DataCoordinatorClient.PreconditionFailedResult =>
                f(ExportDAO.PreconditionFailed)
              case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                f(ExportDAO.NotFound(dataset))
              case DataCoordinatorClient.InvalidRowIdResult =>
                f(ExportDAO.InvalidRowId)
              case DataCoordinatorClient.InternalServerErrorResult(code, tag, msg) =>
                f(ExportDAO.InternalServerError(code, tag, msg))
              case x =>
                f(ExportDAO.InternalServerError("unknown", tag, x.toString))
            }
          } else {
            f(ExportDAO.SchemaInvalidForMimeType)
          }
        case None =>
          f(ExportDAO.NotFound(dataset))
      }
    }

  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

}

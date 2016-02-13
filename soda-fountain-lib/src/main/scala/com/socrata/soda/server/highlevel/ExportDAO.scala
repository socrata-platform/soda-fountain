package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.{Precondition, EntityTag, RequestId}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord, ColumnRecordLike}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.control.ControlThrowable

trait ExportDAO {
  class Retry extends ControlThrowable

  val dateTimeParser = ISODateTimeFormat.dateTimeParser

  def retryable[T](limit: Int /* does not include the initial try */)(f: => T): T = {
    var count = 0
    var done = false
    var result: T = null.asInstanceOf[T]
    do {
      try {
        result = f
        done = true
      } catch {
        case _: Retry =>
          count += 1
          if(count > limit) throw new Exception("Retried too many times")
      }
    } while(!done)
    result
  }
  def retry() = throw new Retry


  def export[T](dataset: ResourceName,
                schemaCheck: Seq[ColumnRecordLike] => Boolean,
                onlyColumns: Seq[ColumnRecordLike],
                precondition: Precondition,
                ifModifiedSince: Option[DateTime],
                limit: Option[Long],
                offset: Option[Long],
                copy: String,
                sorted: Boolean,
                rowId: Option[String],
                requestId: RequestId.RequestId)(f: ExportDAO.Result => T): T

  def lookupDataset(resourceName: ResourceName, copy: Option[Stage]): Option[DatasetRecord]
}

object ExportDAO {
  case class ColumnInfo(id: ColumnId, fieldName: ColumnName, humanName: String, typ: SoQLType)
  case class CSchema(approximateRowCount: Option[Long], dataVersion: Option[Long], lastModified: Option[DateTime], locale: String, pk: Option[ColumnName], rowCount: Option[Long], schema: Seq[ColumnInfo])

  sealed abstract class Result
  sealed abstract class SuccessResult extends Result
  sealed abstract class FailResult extends Result

  // SUCCESS
  case class Success(schema: CSchema, entityTag: Option[EntityTag], rows: Iterator[Array[SoQLValue]]) extends SuccessResult

  // FAIL CASES: DataCoordinator
  case object SchemaInvalidForMimeType extends FailResult
  case class NotModified(etag: Seq[EntityTag]) extends FailResult
  case object PreconditionFailed extends FailResult
  case class NotFound(resourceName: ResourceName) extends FailResult
  case class InternalServerError(code: String, tag: String, data: String) extends FailResult

}

package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.{Precondition, EntityTag, RequestId}
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

trait ExportDAO {
  def export[T](dataset: ResourceName,
                schemaCheck: Seq[ColumnRecordLike] => Boolean,
                onlyColumns: Seq[ColumnRecordLike],
                precondition: Precondition,
                ifModifiedSince: Option[DateTime],
                limit: Option[Long],
                offset: Option[Long],
                includeSystemColumns: Boolean,
                copy: String,
                sorted: Boolean,
                requestId: RequestId.RequestId)(f: ExportDAO.Result => T): T
}

object ExportDAO {
  case class ColumnInfo(id: ColumnId, fieldName: ColumnName, humanName: String, typ: SoQLType)
  case class CSchema(approximateRowCount: Option[Long], dataVersion: Option[Long], lastModified: Option[DateTime], locale: String, pk: Option[ColumnName], rowCount: Option[Long], schema: Seq[ColumnInfo])

  sealed abstract class Result
  case class Success(schema: CSchema, entityTag: Option[EntityTag], rows: Iterator[Array[SoQLValue]]) extends Result
  case object PreconditionFailed extends Result
  case class NotModified(etag: Seq[EntityTag]) extends Result
  case class NotFound(resourceName: ResourceName) extends Result
  case object SchemaInvalidForMimeType extends Result
}

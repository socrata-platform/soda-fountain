package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName

import ExportDAO._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName
import com.socrata.http.server.util.{Precondition, EntityTag}

trait ExportDAO {
  def export[T](dataset: ResourceName, precondition: Precondition, limit: Option[Long], offset: Option[Long], copy: String)(f: Result => T): T
}

object ExportDAO {
  case class ColumnInfo(fieldName: ColumnName, humanName: String, typ: SoQLType)
  case class CSchema(approximateRowCount: Option[Long], locale: String, pk: Option[ColumnName], rowCount: Option[Long], schema: Seq[ColumnInfo])

  sealed abstract class Result
  case class Success(schema: CSchema, entityTag: Option[EntityTag], rows: Iterator[Array[SoQLValue]]) extends Result
  case object PreconditionFailed extends Result
  case class NotModified(etag: Seq[EntityTag]) extends Result
  case object NotFound extends Result
}

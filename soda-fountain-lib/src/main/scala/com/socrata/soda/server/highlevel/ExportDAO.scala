package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName

import ExportDAO._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName

trait ExportDAO {
  def export[T](dataset: ResourceName)(f: Result => T): T
}

object ExportDAO {
  case class ColumnInfo(fieldName: ColumnName, humanName: String, typ: SoQLType)
  case class CSchema(locale: String, pk: Option[ColumnName], schema: Seq[ColumnInfo])

  sealed abstract class Result
  case class Success(schema: CSchema, rows: Iterator[Array[SoQLValue]]) extends Result
  case object NotFound extends Result
}

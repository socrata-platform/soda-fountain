package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import com.rojoma.json.ast.JValue
import RowDAO._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soda.server.highlevel.ExportDAO.CSchema

trait RowDAO {
  def query(dataset: ResourceName, query: String): Result
  def getRow(dataset: ResourceName, rowId: RowSpecifier): Result
  def upsert[T](user: String, dataset: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T
  def replace[T](user: String, dataset: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T
  def deleteRow[T](user: String, dataset: ResourceName, rowId: RowSpecifier)(f: UpsertResult => T): T
}

object RowDAO {
  sealed abstract class Result
  sealed trait UpsertResult
  case class Success(status: Int, body: JValue) extends Result
  case class QuerySuccess(status: Int, schema: ExportDAO.CSchema, body: Iterator[Array[SoQLValue]]) extends Result
  case class RowNotFound(specifier: RowSpecifier) extends Result
  case class StreamSuccess(report: Iterator[JValue]) extends UpsertResult // TODO: Not JValue
  case class DatasetNotFound(dataset: ResourceName) extends Result with UpsertResult
  case class UnknownColumn(column: ColumnName) extends UpsertResult
  case object DeleteWithoutPrimaryKey extends UpsertResult
  case class MaltypedData(column: ColumnName, expected: SoQLType, got: JValue) extends Result with UpsertResult
  case class RowNotAnObject(value: JValue) extends UpsertResult
  case object SchemaOutOfSync extends UpsertResult
}

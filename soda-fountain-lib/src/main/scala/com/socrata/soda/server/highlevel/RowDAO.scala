package com.socrata.soda.server.highlevel

import com.rojoma.json.ast.JValue
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.ReportItem
import com.socrata.soda.clients.datacoordinator.RowUpdate
import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLValue, SoQLType}
import org.joda.time.DateTime

import RowDAO._

trait RowDAO {
  def query(dataset: ResourceName, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String, rowCount: Option[String], secondaryInstance:Option[String]): Result
  def getRow(dataset: ResourceName, precondition: Precondition, ifModifiedSince: Option[DateTime], rowId: RowSpecifier, secondaryInstance:Option[String]): Result
  def upsert[T](user: String, dataset: ResourceName, data: Iterator[RowUpdate])(f: UpsertResult => T): T
  def replace[T](user: String, dataset: ResourceName, data: Iterator[RowUpdate])(f: UpsertResult => T): T
  def deleteRow[T](user: String, dataset: ResourceName, rowId: RowSpecifier)(f: UpsertResult => T): T
}

object RowDAO {
  sealed abstract class Result
  sealed trait UpsertResult
  case class Success(status: Int, body: JValue) extends Result
  case class QuerySuccess(etags: Seq[EntityTag], truthVersion: Long, truthLastModified: DateTime, schema: ExportDAO.CSchema, body: Iterator[Array[SoQLValue]]) extends Result
  case class SingleRowQuerySuccess(etags: Seq[EntityTag], truthVersion: Long, truthLastModified: DateTime, schema: ExportDAO.CSchema, body: Array[SoQLValue]) extends Result
  case class PreconditionFailed(failure: Precondition.Failure) extends Result
  case object TooManyRows extends Result
  case class RowNotFound(specifier: RowSpecifier) extends Result with UpsertResult
  case class StreamSuccess(report: Iterator[ReportItem]) extends UpsertResult
  case class DatasetNotFound(dataset: ResourceName) extends Result with UpsertResult
  case class ComputedColumnNotWritable(column: ColumnName) extends UpsertResult
  case class UnknownColumn(column: ColumnName) extends UpsertResult
  case object DeleteWithoutPrimaryKey extends UpsertResult
  case class InvalidRequest(status: Int, body: JValue) extends Result
  case class MaltypedData(column: ColumnName, expected: SoQLType, got: JValue) extends Result with UpsertResult
  case class RowNotAnObject(value: JValue) extends UpsertResult
  case object SchemaOutOfSync extends UpsertResult
}
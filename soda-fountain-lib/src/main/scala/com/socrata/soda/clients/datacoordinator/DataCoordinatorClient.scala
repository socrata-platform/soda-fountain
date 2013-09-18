package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.util._
import com.rojoma.json.ast._
import com.socrata.soda.server.id.{SecondaryId, DatasetId}
import com.socrata.soda.server.util.schema.SchemaSpec

object DataCoordinatorClient {
  object VersionReport{
    implicit val codec = SimpleJsonCodecBuilder[VersionReport].build("version", _.version)
  }
  class VersionReport(val version: Long)

  sealed abstract class Result
  case class SchemaOutOfDate(newSchema: SchemaSpec) extends Result
  case class Success(report: Iterator[JValue]) extends Result
}

trait DataCoordinatorClient {
  import DataCoordinatorClient._

  def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId)
  def getSchema(datasetId: DatasetId): Option[SchemaSpec]

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (DatasetId, Iterable[JValue])
  def update[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T
  def copy[T](datasetId: DatasetId, schemaHash: String, copyData: Boolean, user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Result => T): T
  def publish[T](datasetId: DatasetId, schemaHash: String, snapshotLimit:Option[Int], user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Result => T): T
  def dropCopy[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Result => T): T
  def deleteAllCopies[T](datasetId: DatasetId, schemaHash: String, user: String)(f: Result => T): T
  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): VersionReport
  def export[T](datasetId: DatasetId, schemaHash: String)(f: Result => T): T
}

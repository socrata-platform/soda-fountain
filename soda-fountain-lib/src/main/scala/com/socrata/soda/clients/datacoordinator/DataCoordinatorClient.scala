package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util._
import com.rojoma.json.v3.ast._
import com.socrata.soda.server.id._
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soda.server.util.schema.SchemaSpec
import com.socrata.http.server.util.{Precondition, EntityTag}
import org.joda.time.DateTime

object DataCoordinatorClient {
  object VersionReport{
    implicit val codec = SimpleJsonCodecBuilder[VersionReport].build("version", _.version)
  }
  case class VersionReport(val version: Long)
  case class ReportMetaData(val datasetId: DatasetId, val version: Long, val lastModified: DateTime)

  sealed abstract class Result
  case class SchemaOutOfDate(newSchema: SchemaSpec) extends Result
  case object CannotDeleteRowId extends Result
  case object PreconditionFailed extends Result
  case class NotModified(etags: Seq[EntityTag]) extends Result
  case object DuplicateValuesInColumn extends Result
  case class IncorrectLifecycleStage(actualStage: String, expectedStage: Set[String]) extends Result
  case class NoSuchRollup(name: RollupName) extends Result
  case class NoSuchRow(id: RowSpecifier) extends Result
  case class UnrefinedUserError(code: String) extends Result // TODO: Refine user errors from DC

  sealed abstract class ReportItem
  case class UpsertReportItem(data: Iterator[JValue] /* Note: this MUST be completely consumed before calling hasNext/next on parent iterator! */) extends ReportItem
  case object OtherReportItem extends ReportItem

  case class Success(report: Iterator[ReportItem], etag: Option[EntityTag], copyNumber: Long, newVersion: Long, lastModified: DateTime) extends Result
  case class Export(json: Iterator[JValue], etag: Option[EntityTag]) extends Result
}

trait DataCoordinatorClient {
  import DataCoordinatorClient._

  def propagateToSecondary(datasetId: DatasetId,
                           secondaryId: SecondaryId,
                           extraHeaders: Map[String, String] = Map.empty)
  def getSchema(datasetId: DatasetId): Option[SchemaSpec]

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US",
             extraHeaders: Map[String, String] = Map.empty) : (ReportMetaData, Iterable[ReportItem])

  def update[T](datasetId: DatasetId,
                schemaHash: String,
                user: String,
                instructions: Iterator[DataCoordinatorInstruction],
                extraHeaders: Map[String, String] = Map.empty)
               (f: Result => T): T

  def copy[T](datasetId: DatasetId,
              schemaHash: String,
              copyData: Boolean,
              user: String,
              instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty,
              extraHeaders: Map[String, String] = Map.empty)
             (f: Result => T): T

  def publish[T](datasetId: DatasetId,
                 schemaHash: String,
                 snapshotLimit:Option[Int],
                 user: String,
                 instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty,
                 extraHeaders: Map[String, String] = Map.empty)
                (f: Result => T): T

  def dropCopy[T](datasetId: DatasetId,
                  schemaHash: String,
                  user: String,
                  instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty,
                  extraHeaders: Map[String, String] = Map.empty)
                 (f: Result => T): T

  def deleteAllCopies[T](datasetId: DatasetId,
                         schemaHash: String,
                         user: String,
                         extraHeaders: Map[String, String] = Map.empty)
                        (f: Result => T): T

  def checkVersionInSecondary(datasetId: DatasetId,
                              secondaryId: SecondaryId,
                              extraHeaders: Map[String, String] = Map.empty): VersionReport

  def export[T](datasetId: DatasetId,
                schemaHash: String,
                columns: Seq[String],
                precondition: Precondition,
                ifModifiedSince: Option[DateTime],
                limit: Option[Long],
                offset: Option[Long],
                copy: String,
                sorted: Boolean,
                extraHeaders: Map[String, String])(f: Result => T): T
}

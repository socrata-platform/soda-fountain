package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.util._
import com.socrata.soda.server.macros.{Tag, BranchCodec}
import com.socrata.soda.server.id.{RowSpecifier, ColumnId, RollupName, DatasetId}
import com.socrata.soda.server.util.schema.SchemaSpec

sealed abstract class PossiblyUnknownDataCoordinatorError
case class UnknownDataCoordinatorError(errorCode: String, data: Map[String, JValue]) extends PossiblyUnknownDataCoordinatorError
sealed abstract class DataCoordinatorError extends PossiblyUnknownDataCoordinatorError {

  def code: String = getClass.getAnnotation(classOf[Tag]).value()

}

@Tag("req.precondition-failed")
case class PreconditionFailed() extends DataCoordinatorError

@Tag("req.not-modified")
case class NotModified() extends DataCoordinatorError

@Tag("req.script.header.mismatched-schema")
case class SchemaMismatch(dataset: DatasetId, schema: SchemaSpec) extends DataCoordinatorError

@Tag("req.export.mismatched-schema")
case class SchemaMismatchForExport(dataset: DatasetId, schema: SchemaSpec) extends DataCoordinatorError

@Tag("req.script.header.missing")
case class EmptyCommandStream() extends DataCoordinatorError

@Tag("req.script.command.non-object")
case class CommandIsNotAnObject() extends DataCoordinatorError

@Tag("req.script.command.missing-field")
case class MissingCommandField(`object`: JObject, field: String) extends DataCoordinatorError

@Tag("req.script.command.invalid-field")
case class InvalidCommandFieldValue(`object`: JObject, field: String, value: JValue) extends DataCoordinatorError

@Tag("req.body.command-too-large")
case class RequestEntityTooLarge(`bytes-without-full-datum`: Int) extends DataCoordinatorError

@Tag("req.body.malformed-json")
case class BodyMalformedJson(row: Int, column: Int) extends DataCoordinatorError

@Tag("create.dataset.invalid-locale")
case class InvalidLocale(locale: String) extends DataCoordinatorError

@Tag("delete.rollup.does-not-exist")
case class NoSuchRollup(name: RollupName) extends DataCoordinatorError

@Tag("update.dataset.does-not-exist")
case class NoSuchDataset(dataset: String) extends DataCoordinatorError

@Tag("update.row-identifier.delete")
case class DeleteOnRowId() extends DataCoordinatorError

@Tag("update.dataset.temporarily-not-writable")
case class CannotAcquireDatasetWriteLock(dataset: DatasetId) extends DataCoordinatorError

@Tag("update.read-only-mode")
case class SystemInReadOnlyMode() extends DataCoordinatorError

@Tag("update.dataset.invalid-state")
case class IncorrectLifecycleStage(dataset: DatasetId,
                                   `actual-stage`: String,
                                   `expected-stage`: Set[String]) extends DataCoordinatorError

@Tag("update.dataset.initial-copy-drop")
case class InitialCopyDrop(dataset: DatasetId) extends DataCoordinatorError

@Tag("update.dataset.operation-after-drop")
case class OperationAfterDrop(dataset: DatasetId) extends DataCoordinatorError

@Tag("update.column.exists")
case class ColumnAlreadyExists(dataset: DatasetId, column: ColumnId) extends DataCoordinatorError

@Tag("update.column.illegal-id")
case class IllegalColumnId(id: String) extends DataCoordinatorError

@Tag("update.row.no-id")
case class MissingRowId(dataset: DatasetId) extends DataCoordinatorError

@Tag("update.row.no-such-id")
case class NoSuchRow(dataset: DatasetId, value: RowSpecifier) extends DataCoordinatorError

@Tag("update.type.unknown")
case class NoSuchColumn(dataset: DatasetId, column: ColumnId) extends DataCoordinatorError

@Tag("update.column.system")
case class InvalidSystemColumnOperation(dataset: DatasetId, column: ColumnId) extends DataCoordinatorError

@Tag("update.row-identifier.already-set")
case class PrimaryKeyAlreadyExists(dataset: DatasetId, column: ColumnId, `existing-column`: ColumnId)
  extends DataCoordinatorError

@Tag("update.row-identifier.invalid-type")
case class InvalidTypeForPrimaryKey(dataset: DatasetId, column: ColumnId, `type`: String) extends DataCoordinatorError

@Tag("update.row-identifier.duplicate-values")
case class DuplicateValuesInColumn(dataset: DatasetId, column: ColumnId) extends DataCoordinatorError

@Tag("update.row-identifier.null-values")
case class NullsInColumn(dataset: DatasetId, column: ColumnId) extends DataCoordinatorError

@Tag("update.row-identifier.not-row-identifier")
case class NotPrimaryKey(dataset: DatasetId, column: ColumnId) extends DataCoordinatorError

// Below this is the boring machinery that makes the above stuff work

object DataCoordinatorError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DataCoordinatorError](TagAndValue("errorCode", "data"))).build
}

object UnknownDataCoordinatorError {
  implicit val jCodec = AutomaticJsonCodecBuilder[UnknownDataCoordinatorError]
}

object PossiblyUnknownDataCoordinatorError {
  implicit val jCodec = SimpleHierarchyCodecBuilder[PossiblyUnknownDataCoordinatorError](NoTag).
    branch[DataCoordinatorError].
    branch[UnknownDataCoordinatorError].
    build
}

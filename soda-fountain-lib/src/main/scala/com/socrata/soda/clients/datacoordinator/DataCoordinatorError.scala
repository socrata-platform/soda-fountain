package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.{NoTag, AutomaticJsonCodecBuilder, TagAndValue, SimpleHierarchyCodecBuilder}
import com.socrata.soda.server.macros.{Tag, BranchCodec}
import com.socrata.soda.server.id.DatasetId
import com.socrata.soda.server.util.schema.SchemaSpec
import com.rojoma.json.v3.ast.JValue

sealed abstract class PossiblyUnknownDataCoordinatorError
case class UnknownDataCoordinatorError(errorCode: String, data: Map[String, JValue]) extends PossiblyUnknownDataCoordinatorError
case class UserErrorReportedByDataCoordinatorError(errorCode: String, data: Map[String, JValue]) extends PossiblyUnknownDataCoordinatorError
sealed abstract class DataCoordinatorError extends PossiblyUnknownDataCoordinatorError

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

@Tag("update.row-identifier.delete")
case class DeleteOnRowId() extends DataCoordinatorError

// Below this is the boring machinery that makes the above stuff work

object DataCoordinatorError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DataCoordinatorError](TagAndValue("errorCode", "data"))).build
}

object UserErrorReportedByDataCoordinatorError {
  implicit val jCodec = AutomaticJsonCodecBuilder[UserErrorReportedByDataCoordinatorError]
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

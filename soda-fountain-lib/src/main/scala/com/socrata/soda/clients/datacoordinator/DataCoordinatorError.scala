package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.util.{NoTag, AutomaticJsonCodecBuilder, TagAndValue, SimpleHierarchyCodecBuilder}
import com.socrata.soda.server.macros.{Tag, BranchCodec}
import com.socrata.soda.server.id.DatasetId
import com.socrata.soda.server.util.schema.SchemaSpec
import com.rojoma.json.ast.JValue

sealed abstract class PossiblyUnknownDataCoordinatorError

case class UnknownDataCoordinatorError(errorCode: String, data: Map[String, JValue]) extends PossiblyUnknownDataCoordinatorError
object UnknownDataCoordinatorError {
  implicit val jCodec = AutomaticJsonCodecBuilder[UnknownDataCoordinatorError]
}

sealed abstract class DataCoordinatorError extends PossiblyUnknownDataCoordinatorError

@Tag("req.script.header.mismatched-schema")
case class SchemaMismatch(dataset: DatasetId, schema: SchemaSpec) extends DataCoordinatorError

@Tag("req.script.header.missing")
case class EmptyCommandStream() extends DataCoordinatorError

object DataCoordinatorError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DataCoordinatorError](TagAndValue("errorCode", "data"))).build
}

object PossiblyUnknownDataCoordinatorError {
  implicit val jCodec = SimpleHierarchyCodecBuilder[PossiblyUnknownDataCoordinatorError](NoTag).
    branch[DataCoordinatorError].
    branch[UnknownDataCoordinatorError].
    build
}

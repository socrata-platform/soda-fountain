package com.socrata.soda.server.persistence

import com.rojoma.json.ast.JObject
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.util.schema.SchemaSpec
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, ColumnSpec}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import org.joda.time.DateTime

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(newRecord: DatasetRecord)
  def removeResource(resourceName: ResourceName)
  def translateResourceName(resourceName: ResourceName): Option[MinimalDatasetRecord]
  def lookupDataset(resourceName: ResourceName): Option[DatasetRecord]

  def resolveSchemaInconsistency(datasetId: DatasetId, newSchema: SchemaSpec)

  def setPrimaryKey(datasetId: DatasetId, pkCol: ColumnId)

  def addColumn(datasetId: DatasetId, columnSpec: ColumnSpec) : ColumnRecord
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnId, newFieldName: ColumnName) : Int
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Unit
  def updateVersionInfo(datasetId: DatasetId, dataVersion: Long, lastModified: DateTime): Unit
}

trait DatasetRecordLike {
  type ColumnRecordT <: ColumnRecordLike

  val resourceName: ResourceName
  val systemId: DatasetId
  val columns: Seq[ColumnRecordT]
  val locale: String
  val schemaHash: String
  val primaryKey: ColumnId
  val truthVersion: Long
  val lastModified: DateTime

  lazy val columnsByName = columns.groupBy(_.fieldName).mapValues(_.head)
  lazy val minimalSchemaByName = columnsByName.mapValues(_.typ)
  lazy val columnsById = columns.groupBy(_.id).mapValues(_.head)
  lazy val minimalSchemaById = columnsById.mapValues(_.typ)
  lazy val schemaSpec = SchemaSpec(schemaHash, locale, primaryKey, minimalSchemaById)
}

trait ColumnRecordLike {
  val id: ColumnId
  val fieldName: ColumnName
  val typ: SoQLType
  val isInconsistencyResolutionGenerated: Boolean
}

case class ComputationStrategyRecord(strategyType: ComputationStrategyType.Value, recompute: Boolean, sourceColumns: Option[Seq[String]], parameters: Option[JObject])
object ComputationStrategyRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[ComputationStrategyRecord]
}

// A minimal dataset record is a dataset record minus the name and description columns,
// which are unnecessary for most operations.
case class MinimalColumnRecord(id: ColumnId, fieldName: ColumnName, typ: SoQLType, isInconsistencyResolutionGenerated: Boolean) extends ColumnRecordLike
object MinimalColumnRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[MinimalColumnRecord]
}
case class MinimalDatasetRecord(resourceName: ResourceName, systemId: DatasetId, locale: String, schemaHash: String, primaryKey: ColumnId, columns: Seq[MinimalColumnRecord], truthVersion: Long, lastModified: DateTime) extends DatasetRecordLike {
  type ColumnRecordT = MinimalColumnRecord
}
object MinimalDatasetRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[MinimalDatasetRecord]
}

case class ColumnRecord(id: ColumnId, fieldName: ColumnName, typ: SoQLType, name: String, description: String, isInconsistencyResolutionGenerated: Boolean, computationStrategy: Option[ComputationStrategyRecord]) extends ColumnRecordLike
object ColumnRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[ColumnRecord]
}
case class DatasetRecord(resourceName: ResourceName, systemId: DatasetId, name: String, description: String, locale: String, schemaHash: String, primaryKey: ColumnId, columns: Seq[ColumnRecord], truthVersion: Long, lastModified: DateTime) extends DatasetRecordLike {
  type ColumnRecordT = ColumnRecord
}
object DatasetRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[DatasetRecord]
}

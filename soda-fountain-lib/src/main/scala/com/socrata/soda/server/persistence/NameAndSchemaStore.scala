package com.socrata.soda.server.persistence

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.util.schema.SchemaSpec
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, ColumnSpec}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import org.joda.time.DateTime
import com.socrata.soda.server.copy.Stage

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(newRecord: DatasetRecord)
  def removeResource(resourceName: ResourceName)
  def translateResourceName(resourceName: ResourceName, copy: Option[Stage] = None): Option[MinimalDatasetRecord]
  def latestCopyNumber(resourceName: ResourceName): Long
  def lookupCopyNumber(resourceName: ResourceName, copy: Option[Stage]): Option[Long]
  def lookupDataset(resourceName: ResourceName, copyNumber: Long): Option[DatasetRecord]
  def lookupDataset(resourceName: ResourceName, copy: Option[Stage]): Option[DatasetRecord] = {
    lookupCopyNumber(resourceName, copy).flatMap(lookupDataset(resourceName, _))
  }

  /**
   * Return all copies most recent first
   */
  def lookupDataset(resourceName: ResourceName): Seq[DatasetRecord]
  def resolveSchemaInconsistency(datasetId: DatasetId, newSchema: SchemaSpec)

  def setPrimaryKey(datasetId: DatasetId, pkCol: ColumnId, copyNumber: Long)

  def addColumn(datasetId: DatasetId, copyNumber: Long, columnSpec: ColumnSpec) : ColumnRecord
  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnId, newFieldName: ColumnName, copyNumber: Long) : Int
  def dropColumn(datasetId: DatasetId, columnId: ColumnId, copyNumber: Long) : Unit
  def updateVersionInfo(datasetId: DatasetId, dataVersion: Long, lastModified: DateTime, stage: Option[Stage], copyNumber: Long, snapshotLimit: Option[Int]): Unit
  def makeCopy(datasetId: DatasetId, copyNumber: Long, dataVersion: Long): Unit
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
  val stage: Option[Stage]
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
  val computationStrategy: Option[ComputationStrategyRecord]
}

case class ComputationStrategyRecord(
   strategyType: ComputationStrategyType.Value,
   recompute: Boolean,
   sourceColumns: Option[Seq[String]],
   parameters: Option[JObject])

object ComputationStrategyRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[ComputationStrategyRecord]
}

// A minimal dataset record is a dataset record minus the name and description columns,
// which are unnecessary for most operations.
case class MinimalColumnRecord(
  id: ColumnId,
  fieldName: ColumnName,
  typ: SoQLType,
  isInconsistencyResolutionGenerated: Boolean,
  computationStrategy: Option[ComputationStrategyRecord] = None)
    extends ColumnRecordLike

object MinimalColumnRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[MinimalColumnRecord]
}
case class MinimalDatasetRecord(
  resourceName: ResourceName,
  systemId: DatasetId,
  locale: String,
  schemaHash: String,
  primaryKey: ColumnId,
  columns: Seq[MinimalColumnRecord],
  truthVersion: Long,
  stage: Option[Stage],
  lastModified: DateTime)
    extends DatasetRecordLike {
  type ColumnRecordT = MinimalColumnRecord
}
object MinimalDatasetRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[MinimalDatasetRecord]
}

case class ColumnRecord(
  id: ColumnId,
  fieldName: ColumnName,
  typ: SoQLType,
  name: String,
  description: String,
  isInconsistencyResolutionGenerated: Boolean,
  computationStrategy: Option[ComputationStrategyRecord])
    extends ColumnRecordLike

object ColumnRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[ColumnRecord]
}

case class DatasetRecord(
  resourceName: ResourceName,
  systemId: DatasetId,
  name: String,
  description: String,
  locale: String,
  schemaHash: String,
  primaryKey: ColumnId,
  columns: Seq[ColumnRecord],
  truthVersion: Long,
  stage: Option[Stage],
  lastModified: DateTime)
    extends DatasetRecordLike {
  type ColumnRecordT = ColumnRecord
}

object DatasetRecord {
  implicit val jCodec = AutomaticJsonCodecBuilder[DatasetRecord]
}

package com.socrata.soda.server.persistence

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.clients.datacoordinator.RollupDatasetRelation
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, CopyId, DatasetHandle, DatasetInternalName, ResourceName, RollupMapId, RollupName}
import com.socrata.soda.server.model.RollupInfo
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.util.schema.SchemaSpec
import com.socrata.soda.server.wiremodels.ColumnSpec
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import com.socrata.thirdparty.json.AdditionalJsonCodecs._
import org.joda.time.DateTime

import scala.collection.immutable.Set
import scala.concurrent.duration.FiniteDuration

// TODO: this needs to expose a notion of transactions
trait NameAndSchemaStore {
  def addResource(newRecord: DatasetRecord)
  def removeResource(resourceName: ResourceName)
  def markResourceForDeletion(resourceName: ResourceName, datetime: Option[DateTime])
  def unmarkResourceForDeletion (resourceName: ResourceName)
  def patchResource(toPatch: ResourceName, newResourceName: ResourceName)
  def translateResourceName(resourceName: ResourceName, stage: Option[Stage] = None, deleted: Boolean = false): Option[MinimalDatasetRecord]
  def latestCopyNumber(resourceName: ResourceName): Long
  def lookupCopyNumber(resourceName: ResourceName, copy: Option[Stage]): Option[Long]
  def latestCopyNumber(resourceName: DatasetRecord): Long

  def latestCopyId(resourceName: ResourceName): CopyId

  def lookupCopyId(resourceName: ResourceName, copy: Option[Stage]): Option[CopyId]

  def latestCopyId(resourceName: DatasetRecord): CopyId
  def lookupDataset(resourceName: ResourceName, copyNumber: Long): Option[DatasetRecord]
  def lookupDataset(resourceName: ResourceName, copy: Option[Stage]): Option[DatasetRecord] = {
    lookupCopyNumber(resourceName, copy).flatMap(lookupDataset(resourceName, _))
  }
  def lookupDroppedDatasets(delay:FiniteDuration): List[MinimalDatasetRecord]
  /**
   * Return all copies most recent first
   */
  def lookupDataset(resourceName: ResourceName): Seq[DatasetRecord]
  def resolveSchemaInconsistency(datasetId: DatasetInternalName, newSchema: SchemaSpec)

  def setPrimaryKey(datasetId: DatasetInternalName, pkCol: ColumnId, copyNumber: Long)

  def addColumn(datasetId: DatasetInternalName, copyNumber: Long, columnSpec: ColumnSpec) : ColumnRecord
  def addComputationStrategy(datasetId: DatasetInternalName, copyNumber: Long, columnSpec: ColumnSpec): ColumnRecord
  def dropComputationStrategy(datasetId: DatasetInternalName, copyNumber: Long, columnSpec: ColumnSpec): ColumnRecord
  def updateColumnFieldName(datasetId: DatasetInternalName, columnId: ColumnId, newFieldName: ColumnName, copyNumber: Long) : Int
  def dropColumn(datasetId: DatasetInternalName, columnId: ColumnId, copyNumber: Long, primaryKeyColId: ColumnId) : Unit
  def updateVersionInfo(datasetId: DatasetInternalName, dataVersion: Long, lastModified: DateTime, stage: Option[Stage], copyNumber: Long): Unit
  def makeCopy(datasetId: DatasetInternalName, copyNumber: Long, dataVersion: Long): Unit

  def bulkDatasetLookup(id: Set[DatasetInternalName], includeDeleted: Boolean = false): Set[ResourceName]

  def withColumnUpdater[T](datasetId: DatasetInternalName, copyNumber: Long, columnId: ColumnId)(f: NameAndSchemaStore.ColumnUpdater => T): T

  def createOrUpdateRollup(resourceName: ResourceName,copyNumber:Long, rollupName: RollupName, soql: String): RollupMapId

  def deleteRollupRelations(resourceName: ResourceName,copyNumber:Long,rollupName: RollupName): Int

  def createRollupRelation(primaryResourceName: ResourceName,primaryCopyNumber:Long,secondaryResourceName: ResourceName,secondaryCopyNumber:Long,rollupName: RollupName): (RollupMapId,CopyId)
  def deleteRollups(resourceName: ResourceName,copyNumber:Long,rollups:Set[RollupName]): Int

  def rollupDatasetRelationByPrimaryDataset(primaryDataset: ResourceName,copyNumber:Long): Set[RollupDatasetRelation]

  def rollupDatasetRelationBySecondaryDataset(secondaryDataset: ResourceName,copyNumber:Long): Set[RollupDatasetRelation]

  def markRollupAccessed(resourceName: ResourceName,copyNumber:Long,rollupName: RollupName):Boolean

  def getRollups(resourceName: ResourceName,copyNumber:Long):Set[RollupInfo]

}

object NameAndSchemaStore {
  trait ColumnUpdater {
    def updateFieldName(newFieldName: ColumnName)
  }
}

trait DatasetRecordLike {
  type ColumnRecordT <: ColumnRecordLike

  val resourceName: ResourceName
  val systemId: DatasetInternalName
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

  def handle = DatasetHandle(systemId, resourceName)
}

trait ColumnRecordLike {
  val id: ColumnId
  val fieldName: ColumnName
  val typ: SoQLType
  val isInconsistencyResolutionGenerated: Boolean
  val computationStrategy: Option[ComputationStrategyRecord]
}

case class ComputationStrategyRecord(
   strategyType: StrategyType,
   sourceColumns: Option[Seq[MinimalColumnRecord]],
   parameters: Option[JObject])

// A minimal dataset record is a dataset record minus the name and description columns,
// which are unnecessary for most operations.
case class MinimalColumnRecord(
  id: ColumnId,
  fieldName: ColumnName,
  typ: SoQLType,
  isInconsistencyResolutionGenerated: Boolean,
  computationStrategy: Option[ComputationStrategyRecord] = None)
    extends ColumnRecordLike

case class MinimalDatasetRecord(
                                 resourceName: ResourceName,
                                 systemId: DatasetInternalName,
                                 locale: String,
                                 schemaHash: String,
                                 primaryKey: ColumnId,
                                 columns: Seq[MinimalColumnRecord],
                                 truthVersion: Long,
                                 stage: Option[Stage],
                                 lastModified: DateTime,
                                 deletedAt: Option[DateTime]= None)
    extends DatasetRecordLike {
  type ColumnRecordT = MinimalColumnRecord
}

case class ColumnRecord(
  id: ColumnId,
  fieldName: ColumnName,
  typ: SoQLType,
  isInconsistencyResolutionGenerated: Boolean,
  computationStrategy: Option[ComputationStrategyRecord])
    extends ColumnRecordLike

case class DatasetRecord(
                          resourceName: ResourceName,
                          systemId: DatasetInternalName,
                          name: String,
                          description: String,
                          locale: String,
                          schemaHash: String,
                          primaryKey: ColumnId,
                          columns: Seq[ColumnRecord],
                          truthVersion: Long,
                          stage: Option[Stage],
                          lastModified: DateTime,
                          deletedAt: Option[DateTime] = None)
    extends DatasetRecordLike {
  type ColumnRecordT = ColumnRecord
}

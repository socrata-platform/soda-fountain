package com.socrata.soda.server.persistence.pg

import java.sql.{Connection, ResultSet, Timestamp, Types}
import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.SodaInternalException
import com.socrata.soda.server.persistence.NameAndSchemaStore.ColumnUpdater

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.io.{JsonReader, JsonReaderException}
import com.rojoma.simplearm.v2._
import com.socrata.soda.clients.datacoordinator.RollupDatasetRelation
import com.socrata.soda.server.copy.{Latest, Published, Stage}
import com.socrata.soda.server.highlevel.csrec
import com.socrata.soda.server.id.{ColumnId, CopyId, DatasetInternalName, ResourceName, RollupMapId, RollupName}
import com.socrata.soda.server.model.RollupInfo
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.ResultSetMapper
import com.socrata.soda.server.util.schema.{SchemaHash, SchemaSpec}
import com.socrata.soda.server.wiremodels.ColumnSpec
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.types.SoQLType

import javax.sql.DataSource
import org.joda.time.DateTime

case class SodaFountainStoreError(message: String) extends SodaInternalException(message)

object PostgresStoreImpl {
  def fetchMinimalColumnsSql (includeColumnFilter: Boolean, isDeleted: Boolean) = {
    val columnFilter = if (includeColumnFilter) "c.column_id = ? AND" else ""
    val deletedFilter = if (!isDeleted) "AND dc.deleted_at is null" else "AND dc.deleted_at is not null"
    s"""SELECT c.column_name,
       |       c.column_id,
       |       c.type_name,
       |       c.is_inconsistency_resolution_generated,
       |       cs.computation_strategy_type,
       |       cs.source_columns,
       |       cs.parameters
       | FROM columns c
       | JOIN dataset_copies dc on dc.id = c.copy_id
       | LEFT JOIN computation_strategies cs
       | ON c.dataset_system_id = cs.dataset_system_id AND c.column_id = cs.column_id AND c.copy_id = cs.copy_id
       | WHERE $columnFilter
        |dc.dataset_system_id = ? AND
        |       dc.copy_number = ?
        |       $deletedFilter
        |       """.stripMargin
  }

  sealed abstract class DatasetKey
  object DatasetKey {
    case object ResourceName extends DatasetKey
    case object InternalName extends DatasetKey
  }
  sealed abstract class DatasetKeyLike[T] {
    def key: DatasetKey
    def keyString(t: T): String
  }
  object DatasetKeyLike {
    implicit object ResourceNameKey extends DatasetKeyLike[ResourceName] {
      def key = DatasetKey.ResourceName
      def keyString(t: ResourceName) = t.caseFolded
    }
    implicit object InternalNameKey extends DatasetKeyLike[DatasetInternalName] {
      def key = DatasetKey.InternalName
      def keyString(t: DatasetInternalName) = t.underlying
    }
  }

  def fetchDatasetSql (key: Option[DatasetKey], copyNumber: Boolean, isDeleted: Boolean) = {
    val resourceNameFilter =
      key match {
        case Some(DatasetKey.ResourceName) => "WHERE d.resource_name_casefolded = ?"
        case Some(DatasetKey.InternalName) => "WHERE d.dataset_system_id = ?"
        case None => ""
      }
    val deletedFilter = if (!isDeleted) "AND d.deleted_at is null AND c.deleted_at is null" else "AND d.deleted_at < now() - (?::INTERVAL)"
    val copyNumberFilter = if (copyNumber) "And c.copy_number = ?" else ""
    s"""SELECT d.resource_name, d.dataset_system_id, d.name, d.description, d.locale, c.schema_hash, d.last_modified, d.deleted_at,
    c.copy_number, c.primary_key_column_id, c.latest_version, c.lifecycle_stage, c.updated_at
    FROM datasets d
    Join dataset_copies c on c.dataset_system_id = d.dataset_system_id
    $resourceNameFilter
    $copyNumberFilter
    $deletedFilter
    ORDER By c.copy_number desc
      """.stripMargin
  }

  // The string_to_array function below is a workaround for Postgres JDBC4
  // which does not implement connection.createArrayOf
  private def compStrategySourceColumnPartialSql =
    """
    ARRAY(SELECT c.column_id
            FROM columns c
            Join dataset_copies dc on dc.dataset_system_id = c.dataset_system_id
             AND dc.id = c.copy_id
           WHERE c.column_name = ANY (string_to_array(?, ','))
             AND dc.dataset_system_id = ?
             AND dc.copy_number = ?)
    """.stripMargin


  val addCompStrategySql =
    s"""
    INSERT INTO computation_strategies
        (dataset_system_id,
         column_id,
         computation_strategy_type,
         source_columns,
         parameters,
         copy_id)
         SELECT ?,
                ?,
                ?,
                $compStrategySourceColumnPartialSql,
                ?,
                id
           FROM dataset_copies
          WHERE dataset_system_id = ?
            And copy_number = ?
            And deleted_at is null
    """.stripMargin

  val dropCompStrategySql =
    s"""
    DELETE FROM computation_strategies
     WHERE dataset_system_id = ?
       And column_id = ?
       And copy_id in (SELECT id FROM dataset_copies
                        WHERE dataset_system_id = ?
                          And copy_number = ?
                          And deleted_at is null)
    """.stripMargin

  private val updateVersionInfoSqls = Seq(
    // update dataset basic stuff
    """
    UPDATE datasets
       SET latest_version = ?, last_modified = ?
     WHERE dataset_system_id = ?
    """,
    // update dataset copy basic stuff
    """
    UPDATE dataset_copies
       SET latest_version = ?, updated_at = ?
     WHERE dataset_system_id = ?
       And copy_number = ?
    """,
    // change previously published to discarded
    """
    UPDATE dataset_copies
       SET lifecycle_stage = 'Discarded',
           deleted_at = now()
     WHERE dataset_system_id = ?
       And deleted_at is null
       And lifecycle_stage = 'Published'
       And ?
    """,
    // take effect only when we discard a copy
    """
    UPDATE dataset_copies
       SET lifecycle_stage = ?,
           deleted_at = case when ? = 'Discarded' then now() else deleted_at end,
           latest_version = ?, updated_at = now()
     WHERE dataset_system_id = ?
       And copy_number = ?
       And deleted_at is null
       And ?
    """)

  val updateVersionInfoSql = updateVersionInfoSqls.map(_.stripMargin).mkString(";")
}

class PostgresStoreImpl(dataSource: DataSource) extends NameAndSchemaStore {

  import PostgresStoreImpl._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresStoreImpl])

  def toTimestamp(time: DateTime): Timestamp = new Timestamp(time.getMillis)
  def toDateTime(time: Timestamp): DateTime = new DateTime(time.getTime)
  def toDateTimeOptional(time:Timestamp): Option[DateTime] =
    Option(time).map { t => new DateTime(t.getTime()) }


  def latestCopyNumber(resourceName: ResourceName): Long = {
    lookupCopyNumber(resourceName, None).getOrElse(throw new Exception("there should always be a latest copy"))
  }

  def latestCopyNumber(ds: DatasetRecord): Long = {
    lookupCopyNumber(ds.resourceName, None).getOrElse(throw new Exception("there should always be a latest copy"))
  }

  def lookupCopyNumber(resourceName: ResourceName, stage: Option[Stage]): Option[Long] = {
    using(dataSource.getConnection()){ connection =>
      using(connection.prepareStatement(
        """
        SELECT dc.copy_number
          FROM datasets d
          Join dataset_copies dc On dc.dataset_system_id = d.dataset_system_id
         WHERE d.resource_name_casefolded = ?
           And dc.id = (SELECT id FROM dataset_copies WHERE dataset_system_id = d.dataset_system_id And %s And deleted_at is null ORDER By copy_number DESC LIMIT 1)
        """.format(latestStageAsNone(stage).map(_ => "lifecycle_stage = ?").getOrElse("lifecycle_stage <> 'Discarded'")))) { stmt =>
        stmt.setString(1, resourceName.caseFolded)
        latestStageAsNone(stage).foreach(s => stmt.setString(2, s.name))
        val rs = stmt.executeQuery()
        if (rs.next()) Option(rs.getLong(1)) else None
      }
    }
  }

  def latestCopyId(resourceName: ResourceName): CopyId = {
    lookupCopyId(resourceName, None).getOrElse(throw new Exception("there should always be a latest copy"))
  }

  def latestCopyId(ds: DatasetRecord): CopyId = {
    lookupCopyId(ds.resourceName, None).getOrElse(throw new Exception("there should always be a latest copy"))
  }

  def lookupCopyId(resourceName: ResourceName, stage: Option[Stage]): Option[CopyId] = {
    using(dataSource.getConnection()) { connection =>
      using(connection.prepareStatement(
        """
        SELECT dc.id
          FROM datasets d
          Join dataset_copies dc On dc.dataset_system_id = d.dataset_system_id
         WHERE d.resource_name_casefolded = ?
           And dc.id = (SELECT id FROM dataset_copies WHERE dataset_system_id = d.dataset_system_id And %s And deleted_at is null ORDER By copy_number DESC LIMIT 1)
        """.format(latestStageAsNone(stage).map(_ => "lifecycle_stage = ?").getOrElse("lifecycle_stage <> 'Discarded'")))) { stmt =>
        stmt.setString(1, resourceName.caseFolded)
        latestStageAsNone(stage).foreach(s => stmt.setString(2, s.name))
        val rs = stmt.executeQuery()
        if (rs.next()) Option(new CopyId(rs.getLong(1))) else None
      }
    }
  }

  def lookupCopyNumber(datasetId: DatasetInternalName, stage: Option[Stage]): Option[Long] = {
    using(dataSource.getConnection()){ connection =>
      using(connection.prepareStatement(
        """
        SELECT copy_number
          FROM dataset_copies
         WHERE id = (SELECT id FROM dataset_copies WHERE dataset_system_id = d.dataset_system_id And %s And deleted_at is null ORDER By copy_number DESC LIMIT 1)
        """.format(latestStageAsNone(stage).map(_ => "lifecycle_stage = ?").getOrElse("lifecycle_stage <> 'Discarded'")))) { stmt =>
        stmt.setString(1, datasetId.underlying)
        latestStageAsNone(stage).foreach(s => stmt.setString(2, s.name))
        val rs = stmt.executeQuery()
        if (rs.next()) Option(rs.getLong(1)) else None
      }
    }
  }

  //TODO: Same issue as fetchMinimalColumn, setting the default isDeleteAt to false might not be such a good idea.I am sure there is a more elegant way to do this
  //than duplicating the functions
  def translateResourceName(resourceName: ResourceName, stage: Option[Stage] = None, isDeleted: Boolean = false): Option[MinimalDatasetRecord] = {
    using(dataSource.getConnection) { connection =>
      val dDeletedFilter = if (!isDeleted) " AND d.deleted_at is null" else " AND d.deleted_at is not null"
      val latestCopyDeletedFilter = if (!isDeleted) "AND latest_dc.deleted_at is null" else "AND latest_dc.deleted_at is not null"
      val lifecycleStageFilter = stage.fold("AND lifecycle_stage <> 'Discarded'")(_ => "AND lifecycle_stage = ?")
      using(connection.prepareStatement(
        s"""
        SELECT d.resource_name, d.dataset_system_id, d.locale, dc.schema_hash, dc.primary_key_column_id,
               d.latest_version, d.last_modified, dc.copy_number, dc.lifecycle_stage, dc.deleted_at
          FROM datasets d
          JOIN dataset_copies dc ON dc.dataset_system_id = d.dataset_system_id
         WHERE d.resource_name_casefolded = ?
           AND dc.id = (SELECT id FROM dataset_copies latest_dc
                        WHERE latest_dc.dataset_system_id = d.dataset_system_id
                        $lifecycleStageFilter
                        $latestCopyDeletedFilter
                        ORDER BY copy_number DESC
                        LIMIT 1)
          $dDeletedFilter"""
      )){ translator =>
        translator.setString(1, resourceName.caseFolded)
        stage.foreach(s => translator.setString(2, s.name))
        val rs = translator.executeQuery()
        if(rs.next()) {
          val datasetId = DatasetInternalName(rs.getString("dataset_system_id"))
          val copyNumber = rs.getLong("copy_number")
          Some(MinimalDatasetRecord(
            new ResourceName(rs.getString("resource_name")),
            datasetId,
            rs.getString("locale"),
            rs.getString("schema_hash"),
            ColumnId(rs.getString("primary_key_column_id")),
            fetchMinimalColumns (connection, datasetId, copyNumber, isDeleted = isDeleted),
            rs.getLong("latest_version"),
            Stage(Some(rs.getString("lifecycle_stage"))),
            toDateTime(rs.getTimestamp("last_modified")),
            toDateTimeOptional((rs.getTimestamp("deleted_at")))
            ))
        } else {
          None
        }
      }
    }
  }

  def lookupDataset(resourceName: ResourceName, copyNumber: Long): Option[DatasetRecord] = {
    val datasets = lookupDataset(resourceName, Some(copyNumber))
    if (datasets.isEmpty) None
    else Some(datasets.head)
  }

  def lookupDataset(internalName: DatasetInternalName, copyNumber: Long): Option[DatasetRecord] = {
    val datasets = lookupDataset(internalName, Some(copyNumber))
    if (datasets.isEmpty) None
    else Some(datasets.head)
  }

  def lookupDataset(resourceName: ResourceName): Seq[DatasetRecord] = lookupDataset(resourceName, None)

  private def lookupDataset[T : DatasetKeyLike](resourceName: T, copyNumber: Option[Long]): Seq[DatasetRecord] = {
    val sql = fetchDatasetSql(key = Some(implicitly[DatasetKeyLike[T]].key),
                              copyNumber = {if (copyNumber.isDefined) true else false},
                              isDeleted = false)

    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)

      using(conn.prepareStatement(
        sql)) { dsQuery =>
        dsQuery.setString(1, implicitly[DatasetKeyLike[T]].keyString(resourceName))
        if(copyNumber.isDefined)
        {
          dsQuery.setString(2, copyNumber.toString())
        }
        copyNumber.foreach(dsQuery.setLong(2, _))

        using(dsQuery.executeQuery()) { dsResult =>
          resultSetToDatasetRecords(conn, dsResult, (rs: ResultSet) => {
            val datasetId = DatasetInternalName(rs.getString("dataset_system_id"))
            val copyNumber = rs.getLong("copy_number")
            DatasetRecord(
              new ResourceName(rs.getString("resource_name")),
              datasetId,
              rs.getString("name"),
              rs.getString("description"),
              rs.getString("locale"),
              rs.getString("schema_hash"),
              ColumnId(rs.getString("primary_key_column_id")),
              fetchFullColumns(conn, datasetId, copyNumber),
              rs.getLong("latest_version"),
              Stage(Some(rs.getString("lifecycle_stage"))),
              toDateTime(rs.getTimestamp("updated_at")))
            }
          )
        }
      }
    }
  }

  @tailrec
  private def resultSetToDatasetRecords(conn: Connection, rs: ResultSet, decode: ResultSet => DatasetRecord,
    acc: Seq[DatasetRecord] = Seq.empty[DatasetRecord]): Seq[DatasetRecord] = {
    if (!rs.next) acc.reverse
    else {
      val r = decode(rs)
      resultSetToDatasetRecords(conn, rs, decode, r +: acc)
    }
  }

  def updateSchemaHash(conn: Connection, datasetId: DatasetInternalName, copyNumber: Long) {
    val (locale, pkcol) = using(conn.prepareStatement(
      """
      SELECT d.locale, c.primary_key_column_id
        FROM datasets d
        Join dataset_copies c on d.dataset_system_id = c.dataset_system_id
       WHERE d.dataset_system_id = ?
         And c.copy_number = ?
         And c.deleted_at is null
      """.stripMargin)){ stmt =>
      stmt.setString(1, datasetId.underlying)
      stmt.setLong(2, copyNumber)
      val rs = stmt.executeQuery()
      if(rs.next()) {
        val locale = rs.getString("locale")
        val pk = ColumnId(rs.getString("primary_key_column_id"))
        (locale, pk)
      } else {
        //huh
        return
      }
    }
    val cols = fetchMinimalColumns(conn, datasetId, copyNumber)
    val hash = SchemaHash.computeHash(locale, pkcol, cols)
    using(conn.prepareStatement("update dataset_copies set schema_hash = ? where dataset_system_id = ? and copy_number = ? And deleted_at is null")) { stmt =>
      stmt.setString(1, hash)
      stmt.setString(2, datasetId.underlying)
      stmt.setLong(3, copyNumber)
      stmt.executeUpdate()
    }
  }

  def resolveSchemaInconsistency(datasetId: DatasetInternalName, newSchema: SchemaSpec) {
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      val schemaHash = using(conn.prepareStatement("select schema_hash from datasets where dataset_system_id = ? for update")) { stmt =>
        stmt.setString(1, datasetId.underlying)
        using(stmt.executeQuery()) { rs =>
          if(!rs.next()) return // huh
          else rs.getString("schema_hash")
        }
      }

      if(schemaHash == newSchema) return // something else got to it first

      val cols = using(conn.prepareStatement("select column_id, type_name from columns where dataset_system_id = ?")) { stmt =>
        stmt.setString(1, datasetId.underlying)
        using(stmt.executeQuery()) { rs =>
          val result = Map.newBuilder[ColumnId, SoQLType]
          while(rs.next()) {
            result += ColumnId(rs.getString("column_id")) -> SoQLType.typesByName(TypeName(rs.getString("type_name")))
          }
          result.result()
        }
      }

      // ok; now we need to figure out what columns to kill, create, or change.
      val toDelete = cols.keySet -- newSchema.schema.keySet
      val toCreate = newSchema.schema.keySet -- cols.keySet
      val toChange = (cols.keySet intersect newSchema.schema.keySet).filter { cid =>
        cols(cid) != newSchema.schema(cid)
      }

      val newSimpleSchema = (cols -- toDelete) ++ (toCreate ++ toChange).iterator.map { cid => cid -> newSchema.schema(cid) }
      assert(SchemaHash.computeHash(newSchema.locale, newSchema.pk, newSimpleSchema) == newSchema.hash, "Computed hash doesn't match the one I'm supposed to replace it with")

      if(toDelete.nonEmpty) {
        using(conn.prepareStatement("delete from columns where dataset_system_id = ? and column_id = ?")) { stmt =>
          for(col <- toDelete) {
            stmt.setString(1, datasetId.underlying)
            stmt.setString(2, col.underlying)
            stmt.addBatch()
          }
          val deleted = stmt.executeBatch()
          assert(deleted.forall(_ == 1), "tried to delete a column but it wasn't there (or there was more than one??)")
        }
      }

      if(toCreate.nonEmpty) {
        // we need our field names and human-oriented column names to be unique.
        // so we'll name them "unknown_$CID" and "Unknown column $CID".
        // If the former conflicts, add a disambiguating number.  IF the latter
        // conflicts, add primes until it doesn't.
        val existingFieldNames = using(conn.prepareStatement("select column_name from columns where dataset_system_id = ?")) { stmt =>
          stmt.setString(1, datasetId.underlying)
          using(stmt.executeQuery()) { rs =>
            val fns = Set.newBuilder[ColumnName]

            while(rs.next()) {
              fns += ColumnName(rs.getString("column_name"))
            }

            fns.result()
          }
        }
        val copyNumber = lookupCopyNumber(datasetId, Some(Latest)).getOrElse(throw new Exception("cannot find the latest copy"))
        addColumns(conn, datasetId, copyNumber, toCreate.iterator.map { cid =>
          var newFieldName = ColumnName("unknown_" + cid.underlying)
          if(existingFieldNames.contains(newFieldName)) {
            var i = 1
            do {
              newFieldName = ColumnName("unknown_" + cid.underlying + "_" + i)
              i += 1
            } while(existingFieldNames.contains(newFieldName))
          }

          // TODO : Understand what needs to happen to computation strategy here
          ColumnRecord(cid, newFieldName, newSchema.schema(cid), isInconsistencyResolutionGenerated = true, None)
        })
      }

      if(toChange.nonEmpty) {
        using(conn.prepareStatement("update columns set type_name = ? where dataset_system_id = ? and column_id = ?")) { stmt =>
          for(col <- toChange) {
            stmt.setString(1, newSchema.schema(col).name.name)
            stmt.setString(2, datasetId.underlying)
            stmt.setString(3, col.underlying)
            stmt.addBatch()
          }
          val changed = stmt.executeBatch()
          assert(changed.forall(_ == 1), "tried to change a column but it wasn't there (or there was more than one??)")
        }
      }

      using(conn.prepareStatement("update datasets set locale = ?, primary_key_column_id = ?, schema_hash = ? where dataset_system_id = ?")) { stmt =>
        stmt.setString(1, newSchema.locale)
        stmt.setString(2, newSchema.pk.underlying)
        stmt.setString(3, newSchema.hash)
        stmt.setString(4, datasetId.underlying)
        stmt.executeUpdate()
      }

      conn.commit()
    }
  }

  def fetchMinimalColumn(conn: Connection, datasetId: DatasetInternalName, columnId: ColumnId, copyNumber: Long): Option[MinimalColumnRecord] = {
    val sql = fetchMinimalColumnsSql(includeColumnFilter = true, isDeleted = false)

    using(conn.prepareStatement(sql)) { colQuery =>
      colQuery.setString(1, columnId.underlying)
      colQuery.setString(2, datasetId.underlying)
      colQuery.setLong(3, copyNumber)
      using (colQuery.executeQuery()) { rs =>
        if (!rs.next) None
        else Some(parseMinimalColumn(conn, datasetId, rs, copyNumber))
      }
    }
  }

  def fetchMinimalColumns (conn: Connection, datasetId: DatasetInternalName, copyNumber: Long, isDeleted: Boolean = false): Seq[MinimalColumnRecord] = {
    val sql = fetchMinimalColumnsSql(includeColumnFilter = false, isDeleted = isDeleted)
    using(conn.prepareStatement(sql)) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      colQuery.setLong(2, copyNumber)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[MinimalColumnRecord]
        while(rs.next()) {
          result += parseMinimalColumn(conn, datasetId, rs, copyNumber)
        }
        result.result()
      }
    }
  }
  def parseMinimalColumn(conn: Connection, datasetId: DatasetInternalName, rs: ResultSet, copyNumber: Long): MinimalColumnRecord =
    MinimalColumnRecord(
      ColumnId(rs.getString("column_id")),
      new ColumnName(rs.getString("column_name")),
      SoQLType.typesByName(TypeName(rs.getString("type_name"))),
      rs.getBoolean("is_inconsistency_resolution_generated")//,
      //fetchComputationStrategy(conn, datasetId, rs, copyNumber)
    )

  def fetchFullColumns(conn: Connection, datasetId: DatasetInternalName, copyNumber: Long): Seq[ColumnRecord] = {
    val sql =
      """SELECT c.column_name,
        |       c.column_id,
        |       c.type_name,
        |       c.is_inconsistency_resolution_generated,
        |       cs.computation_strategy_type,
        |       cs.source_columns,
        |       cs.parameters
        | FROM columns c
        | JOIN dataset_copies dc on dc.id = c.copy_id
        | LEFT JOIN computation_strategies cs
        | ON c.dataset_system_id = cs.dataset_system_id AND c.column_id = cs.column_id AND c.copy_id = cs.copy_id
        | WHERE dc.dataset_system_id = ?
        |   AND dc.copy_number = ?
            And dc.deleted_at is null
      """.stripMargin

    using(conn.prepareStatement(sql)) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      colQuery.setLong(2, copyNumber)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[ColumnRecord]
        while(rs.next()) {
          result += ColumnRecord(
            ColumnId(rs.getString("column_id")),
            new ColumnName(rs.getString("column_name")),
            SoQLType.typesByName(TypeName(rs.getString("type_name"))),
            rs.getBoolean("is_inconsistency_resolution_generated"),
            fetchComputationStrategy(conn, datasetId, rs, copyNumber)
          )
        }
        result.result()
      }
    }
  }

  def fetchComputationStrategy(conn: Connection, datasetId: DatasetInternalName, rs: ResultSet, copyNumber: Long): Option[ComputationStrategyRecord] = {
    def parseStrategyType(raw: String) =
      StrategyType.withName(raw) match {
        case Some(typ) => typ
        case None => throw new SodaFountainStoreError(s"Invalid computation strategy type found in database: '$raw'")
      }

    def parseParameters(raw: String) =
      try {
        JsonReader.fromString(raw) match {
          case params: JObject => params
          case other           => throw new SodaFountainStoreError("Computation strategy source columns could not be parsed")
        }
      } catch {
        case e: JsonReaderException =>
          throw new SodaFountainStoreError("Computation strategy source columns could not be parsed")
      }

    def strategyType = for {
      raw <- Option(rs.getString("computation_strategy_type"))
      typ <- Option(parseStrategyType(raw))
    } yield typ

    def parameters = for {
      raw    <- Option(rs.getString("parameters"))
      params <- Option(parseParameters(raw))
    } yield params

    def sourceColumns = rs.getArray("source_columns") match {
      case arr: java.sql.Array =>
        val columnIds = arr.getArray.asInstanceOf[Array[String]].toSeq // yuk
        Some(columnIds.map(columnId => fetchMinimalColumn(conn, datasetId, ColumnId(columnId), copyNumber)).flatten)
      case _                   =>
        None
    }

    for {
      typ       <- strategyType
      params    <- Option(parameters)
      source    <- Option(sourceColumns)
    } yield ComputationStrategyRecord(typ, source, params)
  }

  def addColumns(connection: Connection, datasetId: DatasetInternalName, copyNumber: Long, columns: TraversableOnce[ColumnRecord]) {
    if(columns.nonEmpty) {
      val addColumnSql =
        """INSERT INTO columns
          |   (dataset_system_id,
          |    column_name_casefolded,
          |    column_name,
          |    column_id,
          |    name,
          |    description,
          |    type_name,
          |    is_inconsistency_resolution_generated,
          |    copy_id)
          | SELECT ?, ?, ?, ?, '', '', ?, ?, id FROM dataset_copies
          |  WHERE dataset_system_id = ? AND copy_number = ?
          |    And deleted_at is null
          | """.stripMargin

      using(connection.prepareStatement(addColumnSql)) { colAdder =>
        for(crec <- columns) {
          // TODO: Ensure the names will fit in the space available
          colAdder.setString(1, datasetId.underlying)
          colAdder.setString(2, crec.fieldName.caseFolded)
          colAdder.setString(3, crec.fieldName.name)
          colAdder.setString(4, crec.id.underlying)
          colAdder.setString(5, crec.typ.name.name)
          colAdder.setBoolean(6, crec.isInconsistencyResolutionGenerated)
          colAdder.setString(7, datasetId.underlying)
          colAdder.setLong(8, copyNumber)
          colAdder.addBatch
        }
        colAdder.executeBatch
      }

      addComputationStrategy(connection, datasetId, copyNumber, columns)
    }
  }

  def addComputationStrategy(connection: Connection, datasetId: DatasetInternalName, copyNumber: Long, columns: TraversableOnce[ColumnRecord]) {
    using (connection.prepareStatement(addCompStrategySql)) { csAdder =>
      for (crec <- columns.filter(col => col.computationStrategy.isDefined)) {
        val cs = crec.computationStrategy.get
        csAdder.setString(1, datasetId.underlying)
        csAdder.setString(2, crec.id.underlying)
        csAdder.setString(3, cs.strategyType.toString)
        cs.sourceColumns match {
          case Some(seq) => csAdder.setString(4, seq.mkString(","))
          case None      => csAdder.setNull(4, Types.ARRAY)
        }
        csAdder.setString(5, datasetId.underlying)
        csAdder.setLong(6, copyNumber)
        cs.parameters match {
          case Some(jObj) => csAdder.setString(7, jObj.toString)
          case None       => csAdder.setNull(7, Types.VARCHAR)
        }
        csAdder.setString(8, datasetId.underlying)
        csAdder.setLong(9, copyNumber)
        csAdder.addBatch()
      }

      csAdder.executeBatch()
    }
  }

  def dropComputationStrategy(connection: Connection, datasetId: DatasetInternalName, copyNumber: Long, columns: TraversableOnce[ColumnRecord]) {
    using (connection.prepareStatement(dropCompStrategySql)) { batch =>
      for (crec <- columns) {
        batch.setString(1, datasetId.underlying)
        batch.setString(2, crec.id.underlying)
        batch.setString(3, datasetId.underlying)
        batch.setLong(4, copyNumber)
        batch.addBatch()
      }

      batch.executeBatch()
    }
  }

  def addResource(newRecord: DatasetRecord): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      using(connection.prepareStatement(
        """
        insert into datasets (resource_name_casefolded, resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id) values(?, ?, ?, ?, ?, ?, ?, ?);
        insert into dataset_copies(dataset_system_id, copy_number, schema_hash, primary_key_column_id, lifecycle_stage, latest_version) values(?, 1, ?, ?, 'Unpublished', 1);
        """)) { adder =>
        // TODO: Ensure the names will fit in the space available
        adder.setString(1, newRecord.resourceName.caseFolded)
        adder.setString(2, newRecord.resourceName.name)
        adder.setString(3, newRecord.systemId.underlying)
        adder.setString(4, newRecord.name)
        adder.setString(5, newRecord.description)
        adder.setString(6, newRecord.locale)
        adder.setString(7, newRecord.schemaHash)
        adder.setString(8, newRecord.primaryKey.underlying)
        adder.setString(9, newRecord.systemId.underlying)
        adder.setString(10, newRecord.schemaHash)
        adder.setString(11, newRecord.primaryKey.underlying)
        adder.execute()
      }
      addColumns(connection, newRecord.systemId, Stage.InitialCopyNumber, newRecord.columns)
      connection.commit()
    }

  def removeResource(resourceName: ResourceName): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      val datasetIdOpt = using(connection.prepareStatement("select dataset_system_id from datasets where resource_name_casefolded = ? for update")) { idFetcher =>
        idFetcher.setString(1, resourceName.caseFolded)
        using(idFetcher.executeQuery()) { rs =>
          if(rs.next()) Some(DatasetInternalName(rs.getString(1)))
          else None
        }
      }
      for(datasetId <- datasetIdOpt) {
        using(connection.prepareStatement(
          """
            delete from computation_strategies where dataset_system_id = ?;
            delete from columns where dataset_system_id = ?;
            delete from rollup_map where dataset_copy_id in (select id from dataset_copies where dataset_system_id = ?);
            delete from rollup_map where id in (select rollup_map_id from rollup_relationship_map where dataset_copy_id in (select id from dataset_copies where dataset_system_id = ?));
            delete from dataset_copies where dataset_system_id = ?;
            delete from datasets where dataset_system_id = ?;
          """)) { deleter =>
          for (i <- 1 to 6) {
            deleter.setString(i, datasetId.underlying)
          }
          deleter.execute()
        }
      }
      connection.commit()
    }

  def markResourceForDeletion (resourceName: ResourceName, datetime: Option[DateTime]): Unit = {
    using(dataSource.getConnection()) { connection =>

      connection.setAutoCommit(false)
      val datasetIdOpt = using(connection.prepareStatement("select dataset_system_id from datasets where resource_name_casefolded = ? for update")) { idFetcher =>
        idFetcher.setString(1, resourceName.caseFolded)
        using(idFetcher.executeQuery()) { rs =>
          if (rs.next()) Some(DatasetInternalName(rs.getString(1)))
          else None
        }
      }
      for (datasetId <- datasetIdOpt) {
        val dt = datetime.getOrElse(DateTime.now())
        using(connection.prepareStatement(
          """ update dataset_copies dc set  deleted_at = ? where dc.dataset_system_id = ?;
              update datasets d set deleted_at = ? where d.dataset_system_id = ?;
        """)) { update =>
          for (i <- 1 to 2) {
            val iBase = 2 * (i - 1)
            update.setTimestamp(iBase + 1, toTimestamp(dt))
            update.setString(iBase + 2, datasetId.underlying)
          }
          update.execute()
        }
    }
      connection.commit()
    }
  }

  def unmarkResourceForDeletion (resourceName: ResourceName): Unit = {
    using(dataSource.getConnection()) { connection =>
      connection.setAutoCommit(false)
      val datasetIdOpt = using(connection.prepareStatement(
        """SELECT
          | d.dataset_system_id
          |FROM datasets d
          |WHERE
          | d.resource_name_casefolded = ? AND d.deleted_at IS NOT NULL
          |FOR UPDATE
        """.stripMargin
      )) { idFetcher =>
        idFetcher.setString(1, resourceName.caseFolded)
        using(idFetcher.executeQuery()) { rs =>
          if (rs.next()) Some(DatasetInternalName(rs.getString(1)))
          else None
        }
      }
      for (datasetId <- datasetIdOpt) {
        using(connection.prepareStatement(
          """UPDATE dataset_copies dc
            | SET deleted_at = NULL
            | WHERE dc.dataset_system_id = ? AND dc.deleted_at IS NOT NULL;
            |
            |UPDATE datasets d
            | SET deleted_at = NULL
            | WHERE d.dataset_system_id = ? AND d.deleted_at IS NOT NULL;""".stripMargin)) {
        update =>
          for (i <- 1 to 2) {
            update.setString(i, datasetId.underlying)
          }
          update.execute()
        }
      }
      connection.commit()
    }
  }

  // WARNING: this method is only suitable for things that are safe to update
  // with no downstream repercussions for data coordinator (eg. name, resource_name)
  def patchResource(toPatch: ResourceName, newResourceName: ResourceName): Unit = {
    val sql =
      """UPDATE datasets
        | SET resource_name = ?,
        |     resource_name_casefolded = ?
        | WHERE resource_name = ?
      """.stripMargin

    using(dataSource.getConnection) { connection =>
      using(connection.prepareStatement(sql)) { update =>
        update.setString(1, newResourceName.name)
        update.setString(2, newResourceName.caseFolded)
        update.setString(3, toPatch.name)
        update.execute()
      }
    }
  }

  def lookupDroppedDatasets(delay: FiniteDuration): List[MinimalDatasetRecord]= {
    val sql = fetchDatasetSql(key = None, copyNumber = false,isDeleted = true)

    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      using(conn.prepareStatement(sql)) {lookup =>
        lookup.setString(1, delay.toSeconds.toString())
        val rs = lookup.executeQuery()
        val result = List.newBuilder[MinimalDatasetRecord]
        while (rs.next()) {
          val datasetId = DatasetInternalName(rs.getString("dataset_system_id"))
          val copyNumber = rs.getLong("copy_number")
          result += MinimalDatasetRecord(
            new ResourceName(rs.getString("resource_name")),
            datasetId,
            rs.getString("locale"),
            rs.getString("schema_hash"),
            ColumnId(rs.getString("primary_key_column_id")),
            fetchMinimalColumns(conn, datasetId, copyNumber),
            rs.getLong("latest_version"),
            Stage(Some(rs.getString("lifecycle_stage"))),
            toDateTime(rs.getTimestamp("last_modified")),
            toDateTimeOptional(rs.getTimestamp("deleted_at"))
          )
        }
        result.result()
      }
    }
  }


  def addColumn(datasetId: DatasetInternalName, copyNumber: Long, spec: ColumnSpec): ColumnRecord =
    using(dataSource.getConnection()) { conn =>
      val result = ColumnRecord(spec.id, spec.fieldName, spec.datatype, isInconsistencyResolutionGenerated = false, spec.computationStrategy.asRecord)
      addColumns(conn, datasetId, copyNumber, Seq(result))
      updateSchemaHash(conn, datasetId, copyNumber)
      result
    }

  def addComputationStrategy(datasetId: DatasetInternalName, copyNumber: Long, spec: ColumnSpec): ColumnRecord =
    using(dataSource.getConnection()) { conn =>
      val result = ColumnRecord(spec.id, spec.fieldName, spec.datatype, isInconsistencyResolutionGenerated = false, spec.computationStrategy.asRecord)
      addComputationStrategy(conn, datasetId, copyNumber, Seq(result))
      // computation strategy is not part of schema hash
      result
    }

  def dropComputationStrategy(datasetId: DatasetInternalName, copyNumber: Long, spec: ColumnSpec): ColumnRecord =
    using(dataSource.getConnection()) { conn =>
      val result = ColumnRecord(spec.id, spec.fieldName, spec.datatype, isInconsistencyResolutionGenerated = false, spec.computationStrategy.asRecord)
      dropComputationStrategy(conn, datasetId, copyNumber, Seq(result))
      // computation strategy is not part of schema hash
      result
    }

  def setPrimaryKey(datasetId: DatasetInternalName, pkCol: ColumnId, copyNumber: Long) {
    using(dataSource.getConnection()) { conn =>
      setPrimaryKeyGuts(conn, datasetId, pkCol, copyNumber)
    }
  }

  private def setPrimaryKeyGuts(conn: Connection, datasetId: DatasetInternalName, pkCol: ColumnId, copyNumber: Long) {
    using(conn.prepareStatement("update dataset_copies set primary_key_column_id = ? where dataset_system_id = ? and copy_number = ?")) { stmt =>
      stmt.setString(1, pkCol.underlying)
      stmt.setString(2, datasetId.underlying)
      stmt.setLong(3, copyNumber)
      stmt.executeUpdate()
      updateSchemaHash(conn, datasetId, copyNumber)
    }
  }

  def updateColumnFieldName(datasetId: DatasetInternalName, columnId: ColumnId, newFieldName: ColumnName, copyNumber: Long): Int = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
          UPDATE columns set column_name = ?, column_name_casefolded = ?
           WHERE column_id = ?
             AND copy_id = (SELECT id FROM dataset_copies WHERE dataset_system_id = ? AND copy_number = ?)
        """.stripMargin)) { stmt =>
        stmt.setString(1, newFieldName.name)
        stmt.setString(2, newFieldName.caseFolded)
        stmt.setString(3, columnId.underlying)
        stmt.setString(4, datasetId.underlying)
        stmt.setLong(5, copyNumber)
        stmt.executeUpdate()
      }
    }
  }

  def updateVersionInfo(datasetId: DatasetInternalName, dataVersion: Long, lastModified: DateTime, stage: Option[Stage], copyNumber: Long) = {
    using(dataSource.getConnection) { conn =>
      using (conn.prepareStatement(updateVersionInfoSql)) { stmt =>
        stmt.setLong(1, dataVersion)
        stmt.setTimestamp(2, toTimestamp(lastModified))
        stmt.setString(3, datasetId.underlying)

        stmt.setLong(4, dataVersion)
        stmt.setTimestamp(5, toTimestamp(lastModified))
        stmt.setString(6, datasetId.underlying)
        stmt.setLong(7, copyNumber)

        stmt.setString(8, datasetId.underlying)
        stmt.setBoolean(9, stage == Some(Published))

        stmt.setString(10, stage.map(_.name).getOrElse(""))
        stmt.setString(11, stage.map(_.name).getOrElse(""))
        stmt.setLong(12, dataVersion)
        stmt.setString(13, datasetId.underlying)
        stmt.setLong(14, copyNumber)
        stmt.setBoolean(15, stage.isDefined)

        stmt.executeUpdate()
      }
    }
  }

  def makeCopy(datasetId: DatasetInternalName, copyNumber: Long, dataVersion: Long) = {
    using(dataSource.getConnection) { conn =>
      using (conn.prepareStatement(
        """
        CREATE TEMP TABLE tmp_last_copy on commit drop as
               SELECT * FROM dataset_copies WHERE dataset_system_id = ? And copy_number < ? And deleted_at is null ORDER By copy_number DESC LIMIT 1;
        INSERT INTO dataset_copies(dataset_system_id, copy_number, schema_hash, latest_version, lifecycle_stage, primary_key_column_id)
               SELECT dataset_system_id, ?, schema_hash, ?, 'Unpublished', primary_key_column_id FROM tmp_last_copy;
        INSERT INTO columns (
               dataset_system_id,
          |    column_name_casefolded,
          |    column_name,
          |    column_id,
          |    name,
          |    description,
          |    type_name,
          |    is_inconsistency_resolution_generated,
          |    copy_id)
          |    SELECT dataset_system_id,
          |           column_name_casefolded,
          |           column_name,
          |           column_id,
          |           '',
          |           '',
          |           type_name,
          |           is_inconsistency_resolution_generated,
          |           (SELECT id FROM dataset_copies WHERE dataset_system_id = ? And copy_number = ?)
          |      FROM columns
          |     WHERE copy_id = (SELECT id FROM tmp_last_copy);
        INSERT INTO computation_strategies (
          |    dataset_system_id,
          |    column_id,
          |    computation_strategy_type,
          |    source_columns,
          |    parameters,
          |    copy_id)
          |    SELECT dataset_system_id,
          |           column_id,
          |           computation_strategy_type,
          |           source_columns,
          |           parameters,
          |           (SELECT id FROM dataset_copies WHERE dataset_system_id = ? And copy_number = ?)
          |      FROM computation_strategies
          |     WHERE copy_id = (SELECT id FROM tmp_last_copy);
        with newCopy as (select * from dataset_copies where dataset_system_id = ? And copy_number = ?)
          |update rollup_map
          |set dataset_copy_id=(select id from newCopy)
          |where dataset_copy_id = (select id FROM tmp_last_copy);
        with newCopy as (select * from dataset_copies where dataset_system_id = ? And copy_number = ?)
          |update rollup_relationship_map
          |set dataset_copy_id=(select id from newCopy)
          |where dataset_copy_id = (select id FROM tmp_last_copy);
        """.stripMargin)) { stmt =>
        stmt.setString(1, datasetId.underlying)
        stmt.setLong(2, copyNumber)
        stmt.setLong(3, copyNumber)
        stmt.setLong(4, dataVersion)
        stmt.setString(5, datasetId.underlying)
        stmt.setLong(6, copyNumber)
        stmt.setString(7, datasetId.underlying)
        stmt.setLong(8, copyNumber)
        stmt.setString(9, datasetId.underlying)
        stmt.setLong(10, copyNumber)
        stmt.setString(11, datasetId.underlying)
        stmt.setLong(12, copyNumber)
        stmt.executeUpdate()
      }
    }
  }

  def dropColumn(datasetId: DatasetInternalName, columnId: ColumnId, copyNumber: Long, pkColId: ColumnId) : Unit = {
    using(dataSource.getConnection) { conn =>
      using(conn.prepareStatement(
        """
          DELETE FROM computation_strategies WHERE column_id = ?
             AND copy_id = (SELECT id FROM dataset_copies WHERE dataset_system_id = ? AND copy_number = ?);
          DELETE FROM columns WHERE column_id = ?
             AND copy_id = (SELECT id FROM dataset_copies WHERE dataset_system_id = ? AND copy_number = ?);
        """)) { stmt =>
        for (i <- 0 to 1) {
          val i2 = 3 * i
          stmt.setString(i2 + 1, columnId.underlying)
          stmt.setString(i2 + 2, datasetId.underlying)
          stmt.setLong(i2 + 3, copyNumber)
        }
        stmt.execute()
        updateSchemaHash(conn, datasetId, copyNumber)
      }
      if (columnId == pkColId) {
        setPrimaryKeyGuts(conn, datasetId, ColumnId(":id"), copyNumber)
      }
    }
  }

  private def latestStageAsNone(stage: Option[Stage]) = {
    stage match {
      case Some(Latest) => None
      case _ => stage
    }
  }

  override def bulkDatasetLookup(ids: Set[DatasetInternalName], includeDeleted: Boolean): Set[ResourceName] =
    if(ids.isEmpty) Set.empty
    else {
      using(dataSource.getConnection) { conn =>
        using(conn.prepareStatement(Iterator.fill(ids.size)("?").mkString(
            s"SELECT resource_name FROM datasets WHERE ${if (!includeDeleted) "deleted_at is NULL AND " else ""} dataset_system_id in (", ",", ")"))) { stmt =>
          ids.iterator.zipWithIndex.foreach { case (id, idx) =>
            stmt.setString(idx + 1, id.underlying)
          }
          using(stmt.executeQuery()) { rs =>
            val result = Set.newBuilder[ResourceName]
            while(rs.next()) {
              result += new ResourceName(rs.getString(1))
            }
            result.result()
          }
        }
      }
    }

  override def withColumnUpdater[T](datasetId: DatasetInternalName, copyNumber: Long, columnId: ColumnId)(f: ColumnUpdater => T): T = {
    using(dataSource.getConnection) { conn =>
      var fieldName: Option[ColumnName] = None

      val columnUpdater = new ColumnUpdater {
        override def updateFieldName(newFieldName: ColumnName): Unit = fieldName = Some(newFieldName)
      }

      val result = f(columnUpdater)

      fieldName.foreach { fieldName =>
        val sql = "UPDATE columns SET column_name = ?, column_name_casefolded = ? WHERE copy_id = (SELECT id FROM dataset_copies WHERE dataset_system_id = ? AND copy_number = ?) AND column_id = ?"
        using(conn.prepareStatement(sql)) { stmt =>
          stmt.setString(1, fieldName.name)
          stmt.setString(2, fieldName.caseFolded)
          stmt.setString(3, datasetId.underlying)
          stmt.setLong(4, copyNumber)
          stmt.setString(5, columnId.underlying)
          stmt.execute()
        }
      }
      result
    }
  }

  override def createOrUpdateRollup(resourceName: ResourceName,copyNumber:Long, rollupName: RollupName, soql: String): RollupMapId = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        //excluded.soql is the original "soql" of the insert event. If copyId and rollupName are the same, lets just update the soql
        """
        insert into rollup_map(dataset_copy_id, name, soql)
        values (
            (
                select c1.id
                from datasets d1
                join dataset_copies c1 on c1.dataset_system_id = d1.dataset_system_id
                where d1.resource_name_casefolded=? and c1.copy_number=?
            ),
            ?,
            ?
        )
        on conflict(dataset_copy_id, name) do update
        set soql=excluded.soql
        returning id
        """.stripMargin)) { stmt =>
        stmt.setString(1, resourceName.caseFolded)
        stmt.setLong(2,copyNumber)
        stmt.setString(3,rollupName.name)
        stmt.setString(4,soql)
        using(stmt.executeQuery()) {
          ResultSetMapper.extractRollupMapId
        }
      }
    }
  }

  override def deleteRollupRelations(resourceName: ResourceName,copyNumber:Long,rollupName: RollupName): Int = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        delete from rollup_relationship_map rrm1
        using rollup_map rm1
        join dataset_copies c1 on rm1.dataset_copy_id = c1.id
        join datasets d1 on c1.dataset_system_id = d1.dataset_system_id
        where rrm1.rollup_map_id=rm1.id
        and d1.resource_name_casefolded = ? and c1.copy_number = ? and rm1.name = ?
        """.stripMargin)) { stmt =>
        stmt.setString(1, resourceName.caseFolded)
        stmt.setLong(2,copyNumber)
        stmt.setString(3,rollupName.name)
        stmt.executeUpdate()
      }
    }
  }

  override def createRollupRelation(primaryResourceName: ResourceName,primaryCopyNumber:Long,secondaryResourceName: ResourceName,secondaryCopyNumber:Long,rollupName: RollupName): (RollupMapId,CopyId) = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        insert into rollup_relationship_map(rollup_map_id, dataset_copy_id)
        values (
            (
                select rm1.id
                from rollup_map rm1
                join dataset_copies c1 on rm1.dataset_copy_id=c1.id
                join datasets d1 on c1.dataset_system_id=d1.dataset_system_id
                where d1.resource_name_casefolded=? and c1.copy_number=? and rm1.name=?
                ),
            (
                select c1.id
                from datasets d1
                join dataset_copies c1 on  d1.dataset_system_id = c1.dataset_system_id
                where d1.resource_name_casefolded=? and c1.copy_number=?
                )
        )
        returning rollup_map_id, dataset_copy_id
        """.stripMargin)) { stmt =>
        stmt.setString(1,primaryResourceName.caseFolded)
        stmt.setLong(2,primaryCopyNumber)
        stmt.setString(3,rollupName.name)
        stmt.setString(4,secondaryResourceName.caseFolded)
        stmt.setLong(5,secondaryCopyNumber)
        using(stmt.executeQuery()) {
          ResultSetMapper.extractRollupMapIdCopyId
        }
      }
    }
  }

  override def deleteRollups(resourceName: ResourceName,copyNumber:Long,rollups:Set[RollupName]): Int ={
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        delete from rollup_map rm1
        using dataset_copies c1
        join datasets d1 on c1.dataset_system_id = d1.dataset_system_id
        where
            rm1.dataset_copy_id = c1.id
            and d1.resource_name_casefolded = ?
            and c1.copy_number = ?
            and rm1.name = any(?)
        """.stripMargin)) { stmt =>
        stmt.setString(1,resourceName.caseFolded)
        stmt.setLong(2,copyNumber)
        stmt.setArray(3, conn.createArrayOf("varchar", rollups.map(_.name).toArray))
        stmt.executeUpdate()
      }
    }
  }

  override def rollupDatasetRelationByPrimaryDataset(primaryDataset: ResourceName,copyNumber:Long): Set[RollupDatasetRelation] = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        select d1.resource_name as primary_dataset,
               rm1.name as name,
               rm1.soql as soql,
               array_agg(d2.resource_name) as secondary_datasets
        from rollup_map rm1
        join dataset_copies c1 on rm1.dataset_copy_id = c1.id
        join datasets d1 on c1.dataset_system_id = d1.dataset_system_id
        join rollup_relationship_map rrm1 on rrm1.rollup_map_id = rm1.id
        join dataset_copies c2 on rrm1.dataset_copy_id = c2.id
        join datasets d2 on c2.dataset_system_id = d2.dataset_system_id
        where d1.resource_name_casefolded=? and c1.copy_number=?
        group by d1.resource_name,rm1.name,rm1.soql
        """.stripMargin)) { stmt =>
        stmt.setString(1, primaryDataset.caseFolded)
        stmt.setLong(2,copyNumber)
        using(stmt.executeQuery()) {
          ResultSetMapper.extractSetRollupDatasetRelation
        }
      }
    }
  }

  override def rollupDatasetRelationBySecondaryDataset(secondaryDataset: ResourceName,copyNumber:Long): Set[RollupDatasetRelation] = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        select d1.resource_name as primary_dataset,
               rm1.name as name,
               rm1.soql as soql,
               array_agg(d2.resource_name) as secondary_datasets
        from rollup_map rm1
        join dataset_copies c1 on rm1.dataset_copy_id = c1.id
        join datasets d1 on c1.dataset_system_id = d1.dataset_system_id
        join rollup_relationship_map rrm1 on rrm1.rollup_map_id = rm1.id
        join dataset_copies c2 on rrm1.dataset_copy_id = c2.id
        join datasets d2 on c2.dataset_system_id = d2.dataset_system_id
        where rm1.id in (
            select rrm1i.rollup_map_id
            from rollup_relationship_map rrm1i
            join dataset_copies c1i on rrm1i.dataset_copy_id = c1i.id
            join datasets d1i on c1i.dataset_system_id = d1i.dataset_system_id
            where d1i.resource_name_casefolded = ? and c1i.copy_number = ?
            )
        group by d1.resource_name,rm1.name,rm1.soql
        """.stripMargin)) { stmt =>
        stmt.setString(1, secondaryDataset.caseFolded)
        stmt.setLong(2,copyNumber)
        using(stmt.executeQuery()) {
          ResultSetMapper.extractSetRollupDatasetRelation
        }
      }
    }
  }

  override def markRollupAccessed(resourceName: ResourceName,copyNumber:Long,rollupName: RollupName):Boolean ={
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        update rollup_map rm1
        set last_accessed=now()
        from dataset_copies c1
        join datasets d1 on c1.dataset_system_id = d1.dataset_system_id
        where rm1.dataset_copy_id = c1.id
        and d1.resource_name_casefolded=? and c1.copy_number=? and rm1.name=?
        """.stripMargin)) { stmt =>
        stmt.setString(1, resourceName.caseFolded)
        stmt.setLong(2,copyNumber)
        stmt.setString(3,rollupName.name)
        stmt.executeUpdate()>0
      }
    }
  }

  override def getRollups(resourceName: ResourceName,copyNumber:Long):Set[RollupInfo]={
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """
        select rm1.*
        from rollup_map rm1
        join dataset_copies c1 on rm1.dataset_copy_id = c1.id
        join datasets d1 on c1.dataset_system_id = d1.dataset_system_id
        where d1.resource_name_casefolded=? and c1.copy_number=?
        """.stripMargin)) { stmt =>
        stmt.setString(1, resourceName.caseFolded)
        stmt.setLong(2,copyNumber)
        using(stmt.executeQuery()) {
          ResultSetMapper.extractSetRollupInfo
        }
      }
    }
  }
}

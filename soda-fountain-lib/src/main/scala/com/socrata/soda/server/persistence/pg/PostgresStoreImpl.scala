package com.socrata.soda.server.persistence.pg

import com.rojoma.json.ast.JObject
import com.rojoma.json.io.JsonReader
import com.rojoma.simplearm.util._
import com.socrata.soda.server.highlevel.csrec
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.schema.{SchemaHash, SchemaSpec}
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, ColumnSpec}
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.soql.types.SoQLType
import java.sql.{Connection, ResultSet, Timestamp, Types}
import javax.sql.DataSource
import org.joda.time.DateTime
import scala.util.Try

case class SodaFountainStoreError(message: String) extends Exception(message)

class PostgresStoreImpl(dataSource: DataSource) extends NameAndSchemaStore {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresStoreImpl])

  def toTimestamp(time: DateTime): Timestamp = new Timestamp(time.getMillis)
  def toDateTime(time: Timestamp): DateTime = new DateTime(time.getTime)

  def translateResourceName(resourceName: ResourceName): Option[MinimalDatasetRecord] = {
    using(dataSource.getConnection()){ connection =>
      using(connection.prepareStatement("select resource_name, dataset_system_id, locale, schema_hash, primary_key_column_id, latest_version, last_modified from datasets where resource_name_casefolded = ?")){ translator =>
        translator.setString(1, resourceName.caseFolded)
        val rs = translator.executeQuery()
        if(rs.next()) {
          val datasetId = DatasetId(rs.getString("dataset_system_id"))
          Some(MinimalDatasetRecord(
            new ResourceName(rs.getString("resource_name")),
            datasetId,
            rs.getString("locale"),
            rs.getString("schema_hash"),
            ColumnId(rs.getString("primary_key_column_id")),
            fetchMinimalColumns(connection, datasetId),
            rs.getLong("latest_version"),
            toDateTime(rs.getTimestamp("last_modified"))
            ))
        } else {
          None
        }
      }
    }
  }

  def lookupDataset(resourceName: ResourceName): Option[DatasetRecord] =
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      using(conn.prepareStatement("select resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id, latest_version, last_modified from datasets where resource_name_casefolded = ?")) { dsQuery =>
        dsQuery.setString(1, resourceName.caseFolded)
        using(dsQuery.executeQuery()) { dsResult =>
          if(dsResult.next()) {
            val datasetId = DatasetId(dsResult.getString("dataset_system_id"))
            Some(DatasetRecord(
              new ResourceName(dsResult.getString("resource_name")),
              datasetId,
              dsResult.getString("name"),
              dsResult.getString("description"),
              dsResult.getString("locale"),
              dsResult.getString("schema_hash"),
              ColumnId(dsResult.getString("primary_key_column_id")),
              fetchFullColumns(conn, datasetId),
              dsResult.getLong("latest_version"),
              toDateTime(dsResult.getTimestamp("last_modified"))
              ))
          } else {
            None
          }
        }
      }
    }

  def updateSchemaHash(conn: Connection, datasetId: DatasetId) {
    val (locale, pkcol) = using(conn.prepareStatement("select locale, primary_key_column_id from datasets where dataset_system_id = ?")){ stmt =>
      stmt.setString(1, datasetId.underlying)
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
    val cols = fetchMinimalColumns(conn, datasetId)
    val hash = SchemaHash.computeHash(locale, pkcol, cols)
    using(conn.prepareStatement("update datasets set schema_hash = ? where dataset_system_id = ?")) { stmt =>
      stmt.setString(1, hash)
      stmt.setString(2, datasetId.underlying)
      stmt.executeUpdate()
    }
  }

  def resolveSchemaInconsistency(datasetId: DatasetId, newSchema: SchemaSpec) {
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
        val (existingFieldNames, existingColumnNames) = using(conn.prepareStatement("select column_name, name from columns where dataset_system_id = ?")) { stmt =>
          stmt.setString(1, datasetId.underlying)
          using(stmt.executeQuery()) { rs =>
            val fns = Set.newBuilder[ColumnName]
            val cns = Set.newBuilder[String]

            while(rs.next()) {
              fns += ColumnName(rs.getString("column_name"))
              cns += rs.getString("name")
            }

            (fns.result(), cns.result())
          }
        }
        addColumns(conn, datasetId, toCreate.iterator.map { cid =>
          var newFieldName = ColumnName("unknown_" + cid.underlying)
          if(existingFieldNames.contains(newFieldName)) {
            var i = 1
            do {
              newFieldName = ColumnName("unknown_" + cid.underlying + "_" + i)
              i += 1
            } while(existingFieldNames.contains(newFieldName))
          }

          var newName = "Unknown column " + cid.underlying
          if(existingColumnNames.contains(newName)) {
            var i = 1
            do {
              newName = "Unknown column " + cid + ("'" * i)
              i += 1
            } while(existingColumnNames.contains(newName))
          }

          // TODO : Understand what needs to happen to computation strategy here
          ColumnRecord(cid, newFieldName, newSchema.schema(cid), newName, "Unknown column discovered by consistency checker", isInconsistencyResolutionGenerated = true, None)
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

  def fetchMinimalColumns(conn: Connection, datasetId: DatasetId): Seq[MinimalColumnRecord] = {
    using(conn.prepareStatement("select column_name, column_id, type_name, is_inconsistency_resolution_generated from columns where dataset_system_id = ?")) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[MinimalColumnRecord]
        while(rs.next()) {
          result += MinimalColumnRecord(
            ColumnId(rs.getString("column_id")),
            new ColumnName(rs.getString("column_name")),
            SoQLType.typesByName(TypeName(rs.getString("type_name"))),
            rs.getBoolean("is_inconsistency_resolution_generated")
          )
        }
        result.result()
      }
    }
  }

  def fetchFullColumns(conn: Connection, datasetId: DatasetId): Seq[ColumnRecord] = {
    val sql =
      """SELECT c.column_name,
        |       c.column_id,
        |       c.type_name,
        |       c.name,
        |       c.description,
        |       c.is_inconsistency_resolution_generated,
        |       cs.computation_strategy_type,
        |       cs.recompute,
        |       cs.source_columns,
        |       cs.parameters
        | FROM columns c
        | LEFT JOIN computation_strategies cs
        | ON c.dataset_system_id = cs.dataset_system_id AND c.column_id = cs.column_id
        | WHERE c.dataset_system_id = ?""".stripMargin

    using(conn.prepareStatement(sql)) { colQuery =>
      colQuery.setString(1, datasetId.underlying)
      using(colQuery.executeQuery()) { rs =>
        val result = Vector.newBuilder[ColumnRecord]
        while(rs.next()) {
          result += ColumnRecord(
            ColumnId(rs.getString("column_id")),
            new ColumnName(rs.getString("column_name")),
            SoQLType.typesByName(TypeName(rs.getString("type_name"))),
            rs.getString("name"),
            rs.getString("description"),
            rs.getBoolean("is_inconsistency_resolution_generated"),
            extractComputationStrategy(rs)
          )
        }
        result.result()
      }
    }
  }

  def extractComputationStrategy(rs: ResultSet): Option[ComputationStrategyRecord] = {
    val strategyType = rs.getString("computation_strategy_type") match {
      case s: String => Try(ComputationStrategyType.withName(s)).toOption match {
        case Some(csType) => csType
        // I don't really like just throwing an exception here, but that seems
        // to be how Soda Fountain deals with unexpected situations currently.
        // It seems better than failing silently.
        case None         => throw new SodaFountainStoreError(s"Invalid computation strategy type found in database: '$s'")
      }
      // Assume that this is not a computed column if no type is specified
      case null      => return None
    }

    val recompute = rs.getBoolean("recompute") // getBoolean will return false if the value is missing in the table

    val sourceColumns = rs.getArray("source_columns") match {
      case arr: java.sql.Array => Some(arr.getArray.asInstanceOf[Array[String]].toSeq)
      case _                   => None
    }

    val parameters = Option(rs.getString("parameters")).map(JsonReader.fromString(_))
    parameters.filterNot(_.isInstanceOf[JObject]).foreach { x =>
      throw new SodaFountainStoreError("Computation strategy source columns could not be parsed")
    }

    Some(ComputationStrategyRecord(strategyType, recompute, sourceColumns, parameters.map(_.asInstanceOf[JObject])))
  }

  def addColumns(connection: Connection, datasetId: DatasetId, columns: TraversableOnce[ColumnRecord]) {
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
          |    is_inconsistency_resolution_generated)
          | VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

      using(connection.prepareStatement(addColumnSql)) { colAdder =>
        for(crec <- columns) {
          log.info("TODO: Ensure the names will fit in the space available")
          colAdder.setString(1, datasetId.underlying)
          colAdder.setString(2, crec.fieldName.caseFolded)
          colAdder.setString(3, crec.fieldName.name)
          colAdder.setString(4, crec.id.underlying)
          colAdder.setString(5, crec.name)
          colAdder.setString(6, crec.description)
          colAdder.setString(7, crec.typ.name.name)
          colAdder.setBoolean(8, crec.isInconsistencyResolutionGenerated)
          colAdder.addBatch
        }
        colAdder.executeBatch
      }

      val addCompStrategySql =
        """INSERT INTO computation_strategies
          |   (dataset_system_id,
          |    column_id,
          |    computation_strategy_type,
          |    recompute,
          |    source_columns,
          |    parameters)
          | VALUES (?, ?, ?, ?, ?, ?)""".stripMargin

      using (connection.prepareStatement(addCompStrategySql)) { csAdder =>
        for (crec <- columns.filter(col => col.computationStrategy.isDefined)) {
          val cs = crec.computationStrategy.get
          csAdder.setString(1, datasetId.underlying)
          csAdder.setString(2, crec.id.underlying)
          csAdder.setString(3, cs.strategyType.toString)
          csAdder.setBoolean(4, cs.recompute)
          cs.sourceColumns match {
            case Some(seq) => csAdder.setArray(5, connection.createArrayOf("text", seq.toArray))
            case None      => csAdder.setNull(5, Types.ARRAY)
          }
          cs.parameters match {
            case Some(jObj) => csAdder.setString(6, jObj.toString)
            case None       => csAdder.setNull(6, Types.VARCHAR)
          }
          csAdder.addBatch
        }

        csAdder.executeBatch
      }
    }
  }

  def addResource(newRecord: DatasetRecord): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      using(connection.prepareStatement("insert into datasets (resource_name_casefolded, resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id) values(?, ?, ?, ?, ?, ?, ?, ?)")){ adder =>
        log.info("TODO: Ensure the names will fit in the space available")
        adder.setString(1, newRecord.resourceName.caseFolded)
        adder.setString(2, newRecord.resourceName.name)
        adder.setString(3, newRecord.systemId.underlying)
        adder.setString(4, newRecord.name)
        adder.setString(5, newRecord.description)
        adder.setString(6, newRecord.locale)
        adder.setString(7, newRecord.schemaHash)
        adder.setString(8, newRecord.primaryKey.underlying)
        adder.execute()
      }
      addColumns(connection, newRecord.systemId, newRecord.columns)
      connection.commit()
    }

  def removeResource(resourceName: ResourceName): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      val datasetIdOpt = using(connection.prepareStatement("select dataset_system_id from datasets where resource_name_casefolded = ? for update")) { idFetcher =>
        idFetcher.setString(1, resourceName.caseFolded)
        using(idFetcher.executeQuery()) { rs =>
          if(rs.next()) Some(DatasetId(rs.getString(1)))
          else None
        }
      }
      for(datasetId <- datasetIdOpt) {
        using(connection.prepareStatement("delete from computation_strategies where dataset_system_id = ?")) { deleter =>
          deleter.setString(1, datasetId.underlying)
          deleter.execute()
        }
        using(connection.prepareStatement("delete from columns where dataset_system_id = ?")) { deleter =>
          deleter.setString(1, datasetId.underlying)
          deleter.execute()
        }
        using(connection.prepareStatement("delete from datasets where dataset_system_id = ?")) { deleter =>
          deleter.setString(1, datasetId.underlying)
          deleter.execute()
        }
      }
      connection.commit()
    }

  def addColumn(datasetId: DatasetId, spec: ColumnSpec): ColumnRecord =
    using(dataSource.getConnection()) { conn =>
      val result = ColumnRecord(spec.id, spec.fieldName, spec.datatype, spec.name, spec.description, isInconsistencyResolutionGenerated = false, spec.computationStrategy.asRecord)
      addColumns(conn, datasetId, Iterator.single(result))
      updateSchemaHash(conn, datasetId)
      result
    }

  def setPrimaryKey(datasetId: DatasetId, pkCol: ColumnId) {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement("update datasets set primary_key_column_id = ? where dataset_system_id = ?")) { stmt =>
        log.info("TODO: Update schemahash too")
        stmt.setString(1, pkCol.underlying)
        stmt.setString(2, datasetId.underlying)
        stmt.executeUpdate()
        updateSchemaHash(conn, datasetId)
      }
    }
  }

  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnId, newFieldName: ColumnName): Int = {
    using(dataSource.getConnection()) { conn =>
      using(conn.prepareStatement("update columns set column_name = ?, column_name_casefolded = ? where dataset_system_id = ? and column_id = ?")) { stmt =>
        stmt.setString(1, newFieldName.name)
        stmt.setString(2, newFieldName.caseFolded)
        stmt.setString(3, datasetId.underlying)
        stmt.setString(4, columnId.underlying)
        stmt.executeUpdate()
      }
    }
  }

  def updateVersionInfo(datasetId: DatasetId, dataVersion: Long, lastModified: DateTime) = {
    using(dataSource.getConnection) { conn =>
      using (conn.prepareStatement("UPDATE datasets SET latest_version = ?, last_modified = ? where dataset_system_id = ?")) { stmt =>
        stmt.setLong(1, dataVersion)
        stmt.setTimestamp(2, toTimestamp(lastModified))
        stmt.setString(3, datasetId.underlying)
        stmt.executeUpdate()
      }
    }
  }

  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Unit = {
    using(dataSource.getConnection) { conn =>
      using(conn.prepareStatement("DELETE FROM computation_strategies WHERE dataset_system_id = ? AND column_id = ?")) { stmt =>
        stmt.setString(1, datasetId.underlying)
        stmt.setString(2, columnId.underlying)
        stmt.execute()
        updateSchemaHash(conn, datasetId)
      }
      using(conn.prepareStatement("DELETE FROM columns WHERE dataset_system_id = ? AND column_id = ?")) { stmt =>
        stmt.setString(1, datasetId.underlying)
        stmt.setString(2, columnId.underlying)
        stmt.execute()
        updateSchemaHash(conn, datasetId)
      }
    }
  }
}

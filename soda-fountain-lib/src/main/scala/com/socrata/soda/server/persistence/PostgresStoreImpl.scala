package com.socrata.soda.server.persistence

import javax.sql.DataSource
import com.rojoma.simplearm.util._
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soql.environment.{TypeName, ColumnName}
import java.sql.Connection
import com.socrata.soql.types.SoQLType
import com.socrata.soda.server.util.schema.{SchemaHash, SchemaSpec}
import com.socrata.soda.server.wiremodels.ColumnSpec

class PostgresStoreImpl(dataSource: DataSource) extends NameAndSchemaStore {
  using(dataSource.getConnection()){ connection =>
    using(connection.createStatement()){ stmt =>
      using(getClass.getClassLoader.getResourceAsStream("db_create.sql")){ stream =>
        val createScript = scala.io.Source.fromInputStream(stream, "UTF-8").getLines().mkString("\n")
        stmt.execute(createScript)
      }
    }
  }

  def translateResourceName(resourceName: ResourceName): Option[MinimalDatasetRecord] = {
    using(dataSource.getConnection()){ connection =>
      using(connection.prepareStatement("select resource_name, dataset_system_id, locale, schema_hash, primary_key_column_id from datasets where resource_name_casefolded = ?")){ translator =>
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
            fetchMinimalColumns(connection, datasetId)))
        } else {
          None
        }
      }
    }
  }

  def lookupDataset(resourceName: ResourceName): Option[DatasetRecord] =
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      using(conn.prepareStatement("select resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id from datasets where resource_name_casefolded = ?")) { dsQuery =>
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
              fetchFullColumns(conn, datasetId)))
          } else {
            None
          }
        }
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
          var newFieldName = ColumnName("unknown_" + cid)
          if(existingFieldNames.contains(newFieldName)) {
            var i = 1
            do {
              newFieldName = ColumnName("unknown_" + cid + "_" + i)
              i += 1
            } while(existingFieldNames.contains(newFieldName))
          }

          var newName = "Unknown column " + cid
          if(existingColumnNames.contains(newName)) {
            var i = 1
            do {
              newName = "Unknown column " + cid + ("'" * i)
              i += 1
            } while(existingColumnNames.contains(newName))
          }

          ColumnRecord(cid, newFieldName, newSchema.schema(cid), newName, "Unknown column discovered by consistency checker", isInconsistencyResolutionGenerated = true)
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
    using(conn.prepareStatement("select column_name, column_id, type_name, name, description, is_inconsistency_resolution_generated from columns where dataset_system_id = ?")) { colQuery =>
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
            rs.getBoolean("is_inconsistency_resolution_generated")
          )
        }
        result.result()
      }
    }
  }

  def addColumns(connection: Connection, datasetId: DatasetId, columns: TraversableOnce[ColumnRecord]) {
    if(columns.nonEmpty) {
      using(connection.prepareStatement("insert into columns (dataset_system_id, column_name_casefolded, column_name, column_id, name, description, type_name, is_inconsistency_resolution_generated) values (?, ?, ?, ?, ?, ?, ?, ?)")) { colAdder =>
        for(cspec <- columns) {
          colAdder.setString(1, datasetId.underlying)
          colAdder.setString(2, cspec.fieldName.caseFolded)
          colAdder.setString(3, cspec.fieldName.name)
          colAdder.setString(4, cspec.id.underlying)
          colAdder.setString(5, cspec.name)
          colAdder.setString(6, cspec.description)
          colAdder.setString(7, cspec.typ.name.name)
          colAdder.setBoolean(8, cspec.isInconsistencyResolutionGenerated)
          colAdder.addBatch()
        }
        colAdder.executeBatch()
      }
    }
  }

  def addResource(newRecord: DatasetRecord): Unit =
    using(dataSource.getConnection()){ connection =>
      connection.setAutoCommit(false)
      using(connection.prepareStatement("insert into datasets (resource_name_casefolded, resource_name, dataset_system_id, name, description, locale, schema_hash, primary_key_column_id) values(?, ?, ?, ?, ?, ?, ?, ?)")){ adder =>
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
      val result = ColumnRecord(spec.id, spec.fieldName, spec.datatype, spec.name, spec.description, isInconsistencyResolutionGenerated = false)
      addColumns(conn, datasetId, Iterator.single(result))
      result
    }

  def updateColumnFieldName(datasetId: DatasetId, columnId: ColumnName, newFieldName: ColumnName) : Unit = ???
  def dropColumn(datasetId: DatasetId, columnId: ColumnId) : Unit = ???
}

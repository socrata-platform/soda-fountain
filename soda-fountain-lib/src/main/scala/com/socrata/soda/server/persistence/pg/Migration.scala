package com.socrata.soda.server.persistence.pg

import java.sql.Connection
import liquibase.Liquibase
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.database.jvm.JdbcConnection

object Migration {
  sealed abstract class MigrationOperation
  case object Migrate extends MigrationOperation
  case class Undo(numChanges: Integer) extends MigrationOperation
  case class Redo(numChanges: Integer) extends MigrationOperation

  def migrateDb(conn: Connection,
                operation: MigrationOperation = Migrate,
                changeLogPath: String = migrationScriptPath)
  {
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor(), new JdbcConnection(conn))
    val database = conn.getCatalog

    operation match {
      case Migrate =>
        liquibase.update(database)
      case Undo(numChanges) =>
        liquibase.rollback(numChanges, database)
      case Redo(numChanges) =>
        liquibase.rollback(numChanges, database)
        liquibase.update(database)
    }
  }

  private val migrationScriptPath = "com/socrata/soda/server/persistence/pg/migrate.xml"
}

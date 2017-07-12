package com.socrata.soda.server.persistence.pg

import java.sql.Connection

import liquibase.Liquibase
import liquibase.lockservice.LockService
import liquibase.resource.ClassLoaderResourceAccessor

object Migration {
  sealed abstract class MigrationOperation
  case object Migrate extends MigrationOperation
  case class Undo(numChanges: Integer) extends MigrationOperation
  case class Redo(numChanges: Integer) extends MigrationOperation

  def migrateDb(conn: Connection,
                operation: MigrationOperation = Migrate,
                changeLogPath: String = migrationScriptPath)
  {
    val jdbc = new NonCommmittingJdbcConnenction(conn)
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor(), jdbc)
    val lockService = LockService.getInstance(liquibase.getDatabase)
    lockService.setChangeLogLockWaitTime(1000 * 3) // 3s where value should be < SQL lock_timeout (30s)

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
    jdbc.realCommit()
  }

  private val migrationScriptPath = "com/socrata/soda/server/persistence/pg/migrate.xml"
}

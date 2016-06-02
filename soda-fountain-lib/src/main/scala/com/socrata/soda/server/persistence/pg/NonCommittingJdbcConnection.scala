package com.socrata.soda.server.persistence.pg

import java.sql.Connection

import liquibase.database.jvm.JdbcConnection

/**
  * liquibase always commits at the end of a changeSet.
  * This class override commit and allows multi-changeSets migration to be commit or rollback
  * as one transaction.
  *
  * liquibase always rollbacks after prerequisite check.  Therefore, rollback is also ignored.
  *
  * When this connection is used for liquibase migration, changeSet.runInTransaction must be true(which is the default).
  * Because this connection is twisted, it should not be used for anything except liquibase migration.
  */
class NonCommmittingJdbcConnenction(conn: Connection) extends JdbcConnection(conn) {

  conn.setAutoCommit(false)

  override def commit(): Unit = { }

  override def rollback(): Unit = { }

  def realCommit(): Unit = conn.commit()
}
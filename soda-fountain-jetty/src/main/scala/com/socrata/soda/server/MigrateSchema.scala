package com.socrata.soda.server

import com.rojoma.simplearm.util._
import com.typesafe.config.ConfigFactory
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.soda.server.persistence.DataSourceFromConfig
import com.socrata.soda.server.persistence.pg.Migration
import com.socrata.soda.server.persistence.pg.Migration.{Migrate, Undo, Redo}

object MigrateSchema extends App {

  override def main(args: Array[String]): Unit = {

    val numChanges = args.length match {
      case 2 => args(1).toInt
      case 1 => 1
      case _ =>
        throw new IllegalArgumentException(
          s"Incorrect number of arguments - expected 1 or 2 but received ${args.length}")
    }

    val operation = args(0).toLowerCase match {
      case "migrate" => Migrate
      case "undo" => Undo(numChanges)
      case "redo" => Redo(numChanges)
      case _ =>
        throw new IllegalArgumentException(s"Unknown migration operation: '${args(0)}'")
    }

    val config = new SodaFountainConfig(ConfigFactory.load)
    for {
      conn <- managed(DataSourceFromConfig(config.database).getConnection)
      stmt <- managed(conn.createStatement())
    } {
      stmt.execute("SET lock_timeout = '30s'")
      Migration.migrateDb(conn, operation)
    }
  }
}

package com.socrata.soda.server.persistence

import com.socrata.soda.server.{SodaFountain, IntegrationTest}
import com.socrata.querycoordinator.client.LocalQueryCoordinatorClient
import com.socrata.soda.server.mocks.LocalDataCoordinator
import dispatch._
import com.socrata.soda.server.types.{DatasetId, ResourceName, ColumnId}
import com.socrata.soql.environment.ColumnName

class PostgresStoreIntegrationTest extends IntegrationTest {

  test("Postgress add/get/remove resourceName and datasetId"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.test @" + time)
    fountain.store.addResource(resourceName, datasetId, Map[ColumnName, ColumnId]())
    val f = fountain.store.translateResourceName(resourceName)
    f match {
      case Some((foundId, columns)) => foundId must equal (datasetId)
      case None => fail("didn't save or find id")
    }
    fountain.store.removeResource(resourceName)
    val f2 = fountain.store.translateResourceName(resourceName)
    f2 match {
      case Some((savedId, columns)) => fail("resource name should have been removed")
      case None => {}
    }
  }

  test("Postgress add/get/remove columnNames and columnIds"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store column integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.column.test @" + time)
    val columns = Map[ColumnName, ColumnId](
      (ColumnName("a b c 1 2 3"), ColumnId("abc123")),
      (ColumnName("d e f 4 5 6"), ColumnId("def456"))
    )
    fountain.store.addResource(resourceName, datasetId, columns)
    val f = fountain.store.translateResourceName(resourceName)
    f match {
      case Some((foundId, foundColumns)) =>
        foundColumns(ColumnName("a b c 1 2 3")) must equal (ColumnId("abc123"))
        foundColumns(ColumnName("d e f 4 5 6")) must equal (ColumnId("def456"))
      case None => fail("didn't find columns")
    }
    fountain.store.removeResource(resourceName)
    val f2 = fountain.store.translateResourceName(resourceName)
    f2 match {
      case Some((savedId, columns)) => fail("resource name should have been removed")
      case None => {}
    }
  }
}

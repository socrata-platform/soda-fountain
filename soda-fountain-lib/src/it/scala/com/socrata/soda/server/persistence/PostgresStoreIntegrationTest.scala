package com.socrata.soda.server.persistence

import com.socrata.soda.server.{SodaFountainForTest, SodaFountainIntegrationTest}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.id._
import com.socrata.soql.types.{SoQLText, SoQLType}

class PostgresStoreIntegrationTest extends SodaFountainIntegrationTest {

  val store : NameAndSchemaStore = SodaFountainForTest.store

  test("Postgress add/get/remove resourceName and datasetId"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.test @" + time)
    val record = new DatasetRecord(resourceName, datasetId, "human name", "human description", "locale string", "mock schema string", new ColumnId("mock column id"), Seq.empty[ColumnRecord])
    store.addResource(record)
    val foundRecord = store.translateResourceName(resourceName)
    foundRecord match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, cols)) =>
        rn must equal (resourceName)
        did must equal (datasetId)
      case None => fail("didn't save or find id")
    }
    store.removeResource(resourceName)
    val f2 = store.translateResourceName(resourceName)
    f2 match {
      case Some(mdr) => fail("resource name should have been removed")
      case None => {}
    }
  }

  test("Postgress add/get/remove columnNames and columnIds"){
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName("postgres name store column integration test @" + time)
    val datasetId = new DatasetId("postgres.name.store.column.test @" + time)
    val columns = Seq[ColumnRecord](
      new ColumnRecord(ColumnId("abc123"), ColumnName("a b c 1 2 3"), SoQLText, "column name human", "column desc human"),
      new ColumnRecord(ColumnId("def456"), ColumnName("d e f 4 5 6"), SoQLText, "column name human", "column desc human")
    )
    val record = new DatasetRecord(resourceName, datasetId, "human name", "human description", "locale string", "mock schema string", new ColumnId("mock column id"), columns)
    store.addResource(record)
    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, Seq(col1, col2))) =>
        col1 must equal (ColumnId("abc123"))
        col2 must equal (ColumnId("def456"))
      case None => fail("didn't find columns")
    }
    store.removeResource(resourceName)
    val f2 = store.translateResourceName(resourceName)
    f2 match {
      case Some(mdr) => fail("resource name should have been removed")
      case None => {}
    }
  }
}

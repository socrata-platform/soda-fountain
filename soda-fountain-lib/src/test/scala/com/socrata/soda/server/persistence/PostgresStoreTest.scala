package com.socrata.soda.server.persistence

import com.rojoma.json.ast.{JString, JObject}
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.id.{ResourceName, DatasetId, ColumnId}
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNumber, SoQLPoint, SoQLText}
import org.scalatest.ShouldMatchers

class PostgresStoreTest extends SodaFountainDatabaseTest with ShouldMatchers with DatasetsForTesting {

  test("Postgres add/get/remove resourceName and datasetId - no columns") {
    val (resourceName, datasetId) = createMockDataset(Seq.empty[ColumnRecord])

    val foundRecord = store.translateResourceName(resourceName)
    foundRecord match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, cols, _, _)) =>
        rn should equal (resourceName)
        did should equal (datasetId)
      case None => fail("didn't save or find id")
    }

    store.removeResource(resourceName)
    val f2 = store.translateResourceName(resourceName)
    f2 match {
      case Some(mdr) => fail("resource name should have been removed")
      case None => {}
    }
  }

  test("Postgres add/get/remove columnNames and columnIds"){
    val columns = Seq[ColumnRecord](
      new ColumnRecord(
        ColumnId("abc123"),
        ColumnName("a b c 1 2 3"),
        SoQLText,
        "column name human",
        "column desc human",
        false,
        None),
      new ColumnRecord(
        ColumnId("def456"),
        ColumnName("d e f 4 5 6"),
        SoQLText,
        "column name human",
        "column desc human",
        false,
        None
      )
    )

    val (resourceName, _) = createMockDataset(columns)

    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, Seq(MinimalColumnRecord(col1, _, _, _, _), MinimalColumnRecord(col2, _, _, _, _)), _, _)) =>
        col1 should equal (ColumnId("abc123"))
        col2 should equal (ColumnId("def456"))
      case None => fail("didn't find columns")
    }

    store.removeResource(resourceName)
    val f2 = store.translateResourceName(resourceName)
    f2 match {
      case Some(mdr) => fail("resource name should have been removed")
      case None => {}
    }
  }

  test("Postgres rename field name"){
    val columns = Seq(ColumnRecord(ColumnId("one"), ColumnName("field_name"), SoQLText, "name", "desc",false, None))
    val (resourceName, datasetId) = createMockDataset(columns)

    store.updateColumnFieldName(datasetId, ColumnId("one"), ColumnName("new_field_name"))
    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky,
        Seq(MinimalColumnRecord(columnId, columnName, _, _, _)), _, _)) =>
        columnId should be (ColumnId("one"))
        columnName should be (ColumnName("new_field_name"))
      case None => fail("didn't find columns")
    }
  }

  test("Postgres validate column storage and retrieval") {
    val columns = Seq[ColumnRecord](
      new ColumnRecord(
        ColumnId("abcd-1234"),
        ColumnName("location"),
        SoQLPoint,
        "Location",
        "Point representing location of the crime",
        false,
        None),
      new ColumnRecord(
        ColumnId("defg-4567"),
        ColumnName("ward"),
        SoQLNumber,
        "Ward",
        "Ward where the crime took place",
        false,
        Some(ComputationStrategyRecord(
          ComputationStrategyType.GeoRegion,
          true,
          Some(Seq("location")),
          Some(JObject(Map("georegion_resource_name" -> JString("chicago_wards"))))
        ))
      )
    )

    val (resourceName, _) = createMockDataset(columns)
    val lookupResult = store.lookupDataset(resourceName)
    lookupResult should not be None
    lookupResult.get.columns.size should be (2)

    lookupResult.get.columns(0) should equal (columns(0))
    lookupResult.get.columns(1) match {
      case ColumnRecord(id,
                        fieldName,
                        typ,
                        displayName,
                        description,
                        isInconsistencyResolutionGenerated,
                        Some(ComputationStrategyRecord(strategy, recompute, Some(sourceColumns), Some(params)))) =>
        id should equal (columns(1).id)
        fieldName should equal (columns(1).fieldName)
        displayName should equal (columns(1).name)
        description should equal (columns(1).description)
        isInconsistencyResolutionGenerated should equal (columns(1).isInconsistencyResolutionGenerated)
        strategy should equal (columns(1).computationStrategy.get.strategyType)
        recompute should equal (columns(1).computationStrategy.get.recompute)
        sourceColumns should equal (Seq(columns(0).id.underlying))
        params should equal (columns(1).computationStrategy.get.parameters.get)
    }
  }

  private def createMockDataset(columns: Seq[ColumnRecord]): (ResourceName, DatasetId) = {
    val dataset = generateDataset("PostgresStoreTest", columns)
    store.addResource(dataset)

    (dataset.resourceName, dataset.systemId)
  }

}
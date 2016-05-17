package com.socrata.soda.server.persistence

import com.rojoma.json.v3.ast.{JObject, JString}
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.copy._
import com.socrata.soda.server.highlevel.csrec
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.wiremodels.{SourceColumnSpec, ComputationStrategySpec, ColumnSpec, ComputationStrategyType}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNumber, SoQLPoint, SoQLText}
import org.joda.time.DateTime
import org.joda.time.chrono.ISOChronology
import org.scalatest.ShouldMatchers

class PostgresStoreTest extends SodaFountainDatabaseTest with ShouldMatchers with DatasetsForTesting {

  test("Postgres add/get/remove resourceName and datasetId - no columns") {
    val (resourceName, datasetId) = createMockDataset(Seq.empty[ColumnRecord])

    val foundRecord = store.translateResourceName(resourceName)
    foundRecord match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, cols, _, stage, _, _)) =>
        stage should equal (Some(Unpublished))
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
        false,
        None),
      new ColumnRecord(
        ColumnId("def456"),
        ColumnName("d e f 4 5 6"),
        SoQLText,
        false,
        None
      )
    )

    val (resourceName, _) = createMockDataset(columns)

    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky, Seq(MinimalColumnRecord(col1, _, _, _, None), MinimalColumnRecord(col2, _, _, _, None)), _, _, _, _)) =>
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
    val columns = Seq(ColumnRecord(ColumnId("one"), ColumnName("field_name"), SoQLText, isInconsistencyResolutionGenerated = false, None))
    val (resourceName, datasetId) = createMockDataset(columns)

    store.updateColumnFieldName(datasetId, ColumnId("one"), ColumnName("new_field_name"), 1L)
    val f = store.translateResourceName(resourceName)
    f match {
      case Some(MinimalDatasetRecord(rn, did, loc, sch, pky,
        Seq(MinimalColumnRecord(columnId, columnName, _, _, _)), _, _, _, _)) =>
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
        false,
        None),
      new ColumnRecord(
        ColumnId("defg-4567"),
        ColumnName("ward"),
        SoQLNumber,
        false,
        Some(ComputationStrategyRecord(
          ComputationStrategyType.GeoRegionMatchOnPoint,
          Some(Seq(MinimalColumnRecord(ColumnId("abcd-1234"), ColumnName("location"), SoQLPoint, isInconsistencyResolutionGenerated = false, None))),
          Some(JObject(Map("georegion_resource_name" -> JString("chicago_wards"))))
        ))
      )
    )

    val (resourceName, _) = createMockDataset(columns)
    val lookupResult = store.lookupDataset(resourceName, Some(Latest))
    lookupResult should not be (None)
    lookupResult.get.columns.size should be (2)

    lookupResult.get.columns(0) should equal (columns(0))
    lookupResult.get.columns(1) match {
      case ColumnRecord(id,
                        fieldName,
                        typ,
                        isInconsistencyResolutionGenerated,
                        Some(ComputationStrategyRecord(strategy, Some(sourceColumns), Some(params)))) =>
        id should equal (columns(1).id)
        fieldName should equal (columns(1).fieldName)
        isInconsistencyResolutionGenerated should equal (columns(1).isInconsistencyResolutionGenerated)
        strategy should equal (columns(1).computationStrategy.get.strategyType)
        sourceColumns should equal (columns(1).computationStrategy.get.sourceColumns.get)
        params should equal (columns(1).computationStrategy.get.parameters.get)
    }
  }

  test("Two copies with different columns"){
    val columnSpecs = Seq(
      ColumnSpec(ColumnId("one"), ColumnName("one"), SoQLText, None),
      ColumnSpec(ColumnId("two"), ColumnName("two"), SoQLText, None)
    )
    val columns = columnSpecs.map(columnSpecTocolumnRecord)
    val (resourceName, datasetId) = createMockDataset(columns.take(1))
    val unpublishedDataVersion = 100L
    store.updateVersionInfo(datasetId, 1L, new DateTime(), Some(Published), 1L, None)
    store.makeCopy(datasetId, 2L, unpublishedDataVersion)
    store.addColumn(datasetId, 2L, columnSpecs(1))

    val publishedCopy = store.lookupDataset(resourceName, Some(Published))
    publishedCopy should not be (None)

    publishedCopy.get.truthVersion should be (1L)
    publishedCopy.get.columns should be (columns.take(1))

    val unpublishedCopy = store.lookupDataset(resourceName, Some(Unpublished))
    unpublishedCopy should not be (None)
    unpublishedCopy.get.truthVersion should be (unpublishedDataVersion)
    unpublishedCopy.get.columns.foreach(println)
    unpublishedCopy.get.columns should be (columns)
  }

  test("Make copy with a computed column") {
    val columnSpecs = Seq(
      ColumnSpec(
        ColumnId("abcd-1234"),
        ColumnName("location"),
        SoQLPoint,
        None
      ),
      ColumnSpec(
        ColumnId("defg-4567"),
        ColumnName("ward"),
        SoQLNumber,
        Some(ComputationStrategySpec(ComputationStrategyType.GeoRegionMatchOnPoint,
          Some(Seq(SourceColumnSpec(ColumnId("abcd-1234"), ColumnName("location")))),
          Some(JObject(Map("georegion_resource_name" -> JString("chicago_wards"))))))
      )
    )

    val columns = Seq[ColumnRecord](
      new ColumnRecord(
        ColumnId("abcd-1234"),
        ColumnName("location"),
        SoQLPoint,
        false,
        None),
      new ColumnRecord(
        ColumnId("defg-4567"),
        ColumnName("ward"),
        SoQLNumber,
        false,
        Some(ComputationStrategyRecord(
          ComputationStrategyType.GeoRegionMatchOnPoint,
          Some(Seq(MinimalColumnRecord(ColumnId("abcd-1234"), ColumnName("location"), SoQLPoint, isInconsistencyResolutionGenerated = false, None))),
          Some(JObject(Map("georegion_resource_name" -> JString("chicago_wards"))))
        ))
      )
    )
    val (resourceName, datasetId) = createMockDataset(columns.take(1))
    val unpublishedDataVersion = 100L
    store.updateVersionInfo(datasetId, 1L, new DateTime(), Some(Published), 1L, None)
    store.makeCopy(datasetId, 2L, unpublishedDataVersion)
    store.addColumn(datasetId, 2L, columnSpecs(1))

    val publishedCopy = store.lookupDataset(resourceName, Some(Published))
    publishedCopy should not be (None)

    publishedCopy.get.truthVersion should be (1L)
    publishedCopy.get.columns should be (columns.take(1))

    val unpublishedCopy = store.lookupDataset(resourceName, Some(Unpublished))
    unpublishedCopy should not be (None)
    unpublishedCopy.get.truthVersion should be (unpublishedDataVersion)
    unpublishedCopy.get.columns.foreach(println)
    unpublishedCopy.get.columns should be (columns)
  }

  test("Publish n times and every copy except the last one should be snapshot"){
    val columnSpecs = sixColumnSpecs
    val columns = columnSpecs.map(columnSpecTocolumnRecord)
    val (resourceName, datasetId) = createMockDataset(columns.take(1))
    store.updateVersionInfo(datasetId, 1L, new DateTime(), Some(Published), 1L, None)

    val totalCopies = columnSpecs.size
    val lastCopy = totalCopies

    for (i <- 1 to totalCopies - 1) {
      val copyNum = i + 1
      val dataVer = copyNum
      store.makeCopy(datasetId, copyNum, 1L)
      store.addColumn(datasetId, copyNum, columnSpecs(i))
      store.updateVersionInfo(datasetId, dataVer, new DateTime(), Some(Published), copyNum, None)
    }

    for (copyNum <- 1 to lastCopy) {
      val ds = store.lookupDataset(resourceName, copyNum).get
      val stage = ds.stage.get
      if (copyNum == lastCopy) stage should be (Published)
      else stage should be (Snapshotted)
    }
  }

  test("Snapshot limit is enforced") {
    val snapshotLimit = 3

    val columnSpecs = sixColumnSpecs
    val columns = columnSpecs.map(columnSpecTocolumnRecord)
    val (resourceName, datasetId) = createMockDataset(columns.take(1))
    store.updateVersionInfo(datasetId, 1L, new DateTime(), Some(Published), 1L, None)

    val totalCopies = columnSpecs.size

    for (i <- 1 to totalCopies - 1) {
      val copyNum = i + 1
      val dataVer = copyNum
      store.makeCopy(datasetId, copyNum, 1L)
      store.addColumn(datasetId, copyNum, columnSpecs(i))
      store.updateVersionInfo(datasetId, dataVer, new DateTime(), Some(Published), copyNum, Some(snapshotLimit))
    }

    val datasets = store.lookupDataset(resourceName)
    datasets.size should be (snapshotLimit + 1) // surviving snapshots + 1 for published copy

    datasets.head.stage should be (Some(Published)) // first copy is published

    datasets.drop(1).foreach { // rest of copies are snapshots
      ds => ds.stage should be (Some(Snapshotted))
    }

    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

    // snapshots have the right info
    val chron = ISOChronology.getInstance()
    val maxDateTime = new DateTime(
      chron.year().getMaximumValue,
      chron.monthOfYear().getMaximumValue,
      chron.dayOfMonth().getMaximumValue,
      chron.hourOfDay().getMaximumValue,
      chron.minuteOfHour().getMaximumValue)
    val published = Tuple3(6, 6, maxDateTime) // number of columns, data version, updated at
    datasets.foldLeft(published) { (state, ds) =>
      ds.columns.size should be (state._1)
      ds.truthVersion should be (state._2)
      ds.lastModified should be < state._3
      Tuple3(state._1 - 1, state._2 - 1, ds.lastModified) // number of columns, data version and updated at are all decreasing.
    }
  }

  test("drop working copies") {
    val columnSpecs = Seq(
      ColumnSpec(ColumnId("one"), ColumnName("one"), SoQLText, None),
      ColumnSpec(ColumnId("two"), ColumnName("two"), SoQLText, None),
      ColumnSpec(ColumnId("three"), ColumnName("three"), SoQLText, None),
      ColumnSpec(ColumnId("four"), ColumnName("four"), SoQLText, None)
    )
    val columns = columnSpecs.map(columnSpecTocolumnRecord)
    val (resourceName, datasetId) = createMockDataset(columns.take(1))
    store.updateVersionInfo(datasetId, 1L, new DateTime(), Some(Published), 1L, Some(10))

    // make working copy
    store.makeCopy(datasetId, 2L, 1L)
    store.addColumn(datasetId, 2L, columnSpecs(1))

    // drop working copy
    store.updateVersionInfo(datasetId, 2L, new DateTime(), Some(Discarded), 2L, None)

    // make another working copy
    store.makeCopy(datasetId, 3L, 3L)
    store.addColumn(datasetId, 3L, columnSpecs(2))
    store.addColumn(datasetId, 3L, columnSpecs(3))
    store.dropColumn(datasetId, ColumnId("four"), 3L)

    val publishedCopy = store.lookupDataset(resourceName, Some(Published))
    publishedCopy should not be (None)
    publishedCopy.get.columns.foreach(println)
    publishedCopy.get.columns should be (columns.take(1))

    val unpublishedCopy = store.lookupDataset(resourceName, Some(Unpublished))
    unpublishedCopy should not be (None)
    val unpublishedColumns = unpublishedCopy.get.columns
    unpublishedColumns.foreach(println)
    unpublishedColumns should be (columns.filter(_.fieldName.name.matches("one|three")))
  }

  test ("dropping a dataset - check if metadata is updated - check if 'deleted-at' column is updated in metadata tables datasets and dataset_copies") {

    val columns = Seq[ColumnRecord](
      new ColumnRecord(
        ColumnId("abc123"),
        ColumnName("a b c 1 2 3"),
        SoQLText,
        false,
        None),
      new ColumnRecord(
        ColumnId("def456"),
        ColumnName("d e f 4 5 6"),
        SoQLText,
        false,
        None
      )
    )
    val (resourceName, _) = createMockDataset(columns)
    store.markResourceForDeletion(resourceName)
    val f = store.translateResourceName(resourceName, isDeleted = true)
    f match {
      case Some(record) =>
        record.deletedAt should be ('defined)
      case None => fail("deleted_at column was not set in datasets")
    }
  }

  test ("deleting a dataset - check if metadata is removed") {
    val columns = Seq[ColumnRecord](
      new ColumnRecord(
        ColumnId("abc123"),
        ColumnName("a b c 1 2 3"),
        SoQLText,
        false,
        None),
      new ColumnRecord(
        ColumnId("def456"),
        ColumnName("d e f 4 5 6"),
        SoQLText,
        false,
        None
      )
    )
    val (resourceName, _) = createMockDataset(columns)
    store.markResourceForDeletion(resourceName)
    store.removeResource(resourceName)
    val f = store.translateResourceName(resourceName, isDeleted = true)
    f match {
      case Some (record) =>
            fail ("metadata was not deleted")
      case None =>
        None
      }

    }

  private def createMockDataset(columns: Seq[ColumnRecord]): (ResourceName, DatasetId) = {
    val dataset = generateDataset("PostgresStoreTest", columns)
    store.addResource(dataset)

    (dataset.resourceName, dataset.systemId)
  }

  private def columnSpecTocolumnRecord(spec: ColumnSpec) = {
    ColumnRecord(spec.id, spec.fieldName, spec.datatype, isInconsistencyResolutionGenerated = false, spec.computationStrategy.asRecord)
  }

  private val sixColumnSpecs = Seq(
    ColumnSpec(ColumnId("zero"), ColumnName("zero"), SoQLText, None),
    ColumnSpec(ColumnId("one"), ColumnName("one"), SoQLText, None),
    ColumnSpec(ColumnId("two"), ColumnName("two"), SoQLText, None),
    ColumnSpec(ColumnId("three"), ColumnName("three"), SoQLText, None),
    ColumnSpec(ColumnId("four"), ColumnName("four"), SoQLText, None),
    ColumnSpec(ColumnId("five"), ColumnName("five"), SoQLText, None)
  )
}
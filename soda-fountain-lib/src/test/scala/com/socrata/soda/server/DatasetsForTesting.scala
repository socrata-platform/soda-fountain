package com.socrata.soda.server

import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence.{SodaFountainDatabaseTest, DatasetRecord, ColumnRecord}
import org.joda.time.DateTime
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLVersion, SoQLID, SoQLText}

trait DatasetsForTesting {
  def mockDataset(humanReadableTestIdentifier: String, columns: Seq[ColumnRecord]): DatasetRecord = {
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName(s"$humanReadableTestIdentifier @$time")
    val datasetId = new DatasetId(s"id @$time")

    new DatasetRecord(
      resourceName,
      datasetId,
      "human name",
      "human description",
      "locale string",
      "mock schema string",
      new ColumnId("mock column id"),
      columns,
      0,
      new DateTime(0))
  }
}

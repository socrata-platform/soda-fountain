package com.socrata.soda.server

import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence._
import org.joda.time.DateTime
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import scala.Some
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.rojoma.json.ast.{JString, JObject}
import com.rojoma.json.ast.JString
import scala.Some
import com.socrata.soda.server.highlevel.ExportDAO
import com.rojoma.json.ast.JString
import scala.Some

trait DatasetsForTesting {
  implicit class DatasetHelpers(val ds: MinimalDatasetRecord) {
    def col(name: String): ColumnRecordLike = ds.columnsByName.get(ColumnName(name)).get
    def colName(name: String): ColumnName = col(name).fieldName
    def colId(name: String): String = col(name).id.underlying
  }

  def generateDataset(humanReadableTestIdentifier: String, columns: Seq[ColumnRecord]): DatasetRecord = {
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

  object TestDatasetWithComputedColumn {

    val idColumn = MinimalColumnRecord(
      ColumnId(":id"),
      ColumnName(":id"),
      SoQLID,
      isInconsistencyResolutionGenerated =  false
    )

    val sourceColumn = MinimalColumnRecord(
      ColumnId("src1-2345"),
      ColumnName("source"),
      SoQLText,
      isInconsistencyResolutionGenerated =  false
    )

    val computationStrategy = ComputationStrategyRecord(
      ComputationStrategyType.Test,
      true,
      Some(Seq(sourceColumn.id.underlying)),
      Some(JObject(Map("concat_text" -> JString("fun")))))

    val computedColumn = MinimalColumnRecord(
      ColumnId("comp-1234"),
      ColumnName(":computed"),
      SoQLText,
      isInconsistencyResolutionGenerated =  false,
      Some(computationStrategy)
    )

    val dataset = MinimalDatasetRecord(
      new ResourceName("test_resource"),
      new DatasetId("abcd-1234"),
      "en_US",
      "095c0a28ba0a9a0e58f22bf456fc82d27853c1b9",
      new ColumnId(":id"),
      Seq(idColumn, sourceColumn, computedColumn),
      9,
      DateTime.now
    )

    val dcColumns = dataset.columns.map { col => ExportDAO.ColumnInfo(col.id, col.fieldName, "Human Readable Name", col.typ) }
    val dcSchema = ExportDAO.CSchema(
      Some(3), Some(2), Some(DateTime.now), "en_US", Some(ColumnName(":id")), Some(3), dcColumns)

    val dcRows = Iterator(Array[SoQLValue](SoQLID(1), SoQLText("giraffe")),
                          Array[SoQLValue](SoQLID(2), SoQLText("marmot")),
                          Array[SoQLValue](SoQLID(3), SoQLText("axolotl")))
  }
}

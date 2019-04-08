package com.socrata.soda.server

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import org.joda.time.DateTime

trait DatasetsForTesting {
  implicit class DatasetHelpers(val ds: DatasetRecord) {
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
      0,
      None,
      new DateTime(0))
  }

  object TestDatasetWithComputedColumn {

    val idColumn = ColumnRecord(
      ColumnId(":id"),
      ColumnName(":id"),
      SoQLID,
      isInconsistencyResolutionGenerated =  false,
      None
    )

    val sourceColumn = ColumnRecord(
      ColumnId("src1-2345"),
      ColumnName("source"),
      SoQLText,
      isInconsistencyResolutionGenerated =  false,
      None
    )

    val computationStrategy = ComputationStrategyRecord(
      StrategyType.Test,
      Some(Seq(MinimalColumnRecord(sourceColumn.id, sourceColumn.fieldName, SoQLNull, isInconsistencyResolutionGenerated = false, None))),
      Some(JObject(Map("concat_text" -> JString("fun")))))

    val computedColumn = ColumnRecord(
      ColumnId("comp-1234"),
      ColumnName(":computed"),
      SoQLText,
      isInconsistencyResolutionGenerated =  false,
      Some(computationStrategy)
    )

    val dataset = DatasetRecord(
      new ResourceName("test_resource"),
      new DatasetId("abcd-1234"),
      "Test resource",
      "Description",
      "en_US",
      "095c0a28ba0a9a0e58f22bf456fc82d27853c1b9",
      new ColumnId(":id"),
      Seq(idColumn, sourceColumn, computedColumn),
      1,
      9,
      None,
      DateTime.now
    )

    val dcColumns = dataset.columns.map { col =>
      val csData = col.computationStrategy.map { cs =>
        import com.rojoma.json.v3.interpolation._
        val sourceColumns = cs.sourceColumns.getOrElse(Nil).map(_.id)
        json""" {
          type: ${cs.strategyType},
          source_columns: ${sourceColumns},
          parameters: ${cs.parameters.getOrElse(JObject.canonicalEmpty)}
        }"""
      }
      ExportDAO.ColumnInfo(col.id, col.fieldName, col.typ, csData)
    }
    val dcSchema = ExportDAO.CSchema(
      Some(3), Some(2), Some(DateTime.now), "en_US", Some(ColumnName(":id")), Some(3), dcColumns)

    val dcRows = Iterator(Array[SoQLValue](SoQLID(1), SoQLText("giraffe")),
                          Array[SoQLValue](SoQLID(2), SoQLText("marmot")),
                          Array[SoQLValue](SoQLID(3), SoQLText("axolotl")))
  }
}

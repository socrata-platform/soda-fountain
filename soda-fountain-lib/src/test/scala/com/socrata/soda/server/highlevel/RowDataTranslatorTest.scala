package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast._
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.highlevel.RowDataTranslator._
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime
import org.scalatest.{Matchers, FunSuite}

class RowDataTranslatorTest extends FunSuite with Matchers with DatasetsForTesting {
  val ds = TestDatasetWithComputedColumn.dataset
  val dsInfo = TestDatasetWithComputedColumn
  val translator = new RowDataTranslator("a-request-id", ds, false)

  test("getInfoForColumnList") {
    val info = translator.getInfoForColumnList(Seq(ds.colId("source"), ds.colId(":id")))
    info should equal (Seq(ds.col("source"), ds.col(":id")))
  }

  test("getInfoForColumnList - bad column ID") {
    a [UnknownColumnEx] should be thrownBy
      translator.getInfoForColumnList(Seq(ds.colId(":id"), "mumbo-jumbo"))
  }

  test("transformClientRowsForUpsert - no rows") {
    val result = translator.transformClientRowsForUpsert(Iterator())
    result.toSeq should equal (Seq())
  }

  test("transformClientRowsForUpsert - upsert contains value for computed column") {
    val rows = Iterator(JObject(Map("source"    -> JString("foo"),
                                    ":computed" -> JString("bar"))))
  }

  test("transformClientRowsForUpsert - upsert contains wrong data type") {
    val rows = Iterator(JObject(Map("source" -> JObject(Map.empty))))

    a [MaltypedDataEx] should be thrownBy
      translator.transformClientRowsForUpsert(rows).next()
  }

  test("transformClientRowsForUpsert - convert json number to text") {
    val rows = Iterator(JObject(Map("source" -> JNumber(3.33))),
                        JObject(Map("source" -> JString("3.33"))))
    val result = translator.transformClientRowsForUpsert(rows)

    result.toSeq should equal (Seq(
      UpsertRow(Map(ds.colId("source") -> JString("3.33"))),
      UpsertRow(Map(ds.colId("source") -> JString("3.33")))
    ))
  }

  test("transformClientRowsForUpsert - delete as ID array") {
    val rows = Iterator(JArray(Seq(JString("row-7k7u_jfib~g6vw"))))
    val result = translator.transformClientRowsForUpsert(rows)

    result.toSeq should equal (Seq(DeleteRow(JString("row-7k7u_jfib~g6vw"))))
  }

  test("transformClientRowsForUpsert - delete as legacy delete") {
    val rows = Iterator(JObject(Map(":id" -> JString("row-7k7u_jfib~g6vw"), ":deleted" -> JBoolean(true))))
    val result = translator.transformClientRowsForUpsert(rows)

    result.toSeq should equal (Seq(DeleteRow(JString("row-7k7u_jfib~g6vw"))))
  }

  test("transformClientRowsForUpsert - mixture of upserts and deletes") {
    val rows = Iterator(JObject(Map("source" -> JString("foo"))),
                        JArray(Seq(JString("row-7k7u_jfib~g6vw"))))
    val result = translator.transformClientRowsForUpsert(rows)

    result.toSeq should equal (Seq(
      UpsertRow(Map(ds.colId("source") -> JString("foo"))),
      DeleteRow(JString("row-7k7u_jfib~g6vw"))
    ))
  }

  test("transformDcRowsForUpsert - nothing to compute") {
    val colsExceptComputed = dsInfo.dcColumns.filterNot(_.fieldName.name == ":computed")
    val schema = ExportDAO.CSchema(
      Some(3), Some(2), Some(DateTime.now), "en_US", Some(ColumnName(":id")), Some(3), colsExceptComputed)
    val result = translator.transformDcRowsForUpsert(schema, dsInfo.dcRows.take(1))

    result.toSeq should equal (Seq(
      UpsertRow(Map(ds.colId(":id") -> JString("row-7nu6~cenw_bx9a"), ds.colId("source") -> JString("giraffe")))
    ))
  }

  test("transformDcRowsForUpsert - one column to compute should not get computed") {
    val colsExceptComputed = dsInfo.dcColumns.filterNot(_.fieldName.name == ":computed")
    val schema = ExportDAO.CSchema(
      Some(3), Some(2), Some(DateTime.now), "en_US", Some(ColumnName(":id")), Some(3), colsExceptComputed)
    val result = translator.transformDcRowsForUpsert(schema, dsInfo.dcRows.take(1))

    result.toSeq should equal (Seq(
      UpsertRow(Map(ds.colId(":id") -> JString("row-7nu6~cenw_bx9a"),
                    ds.colId("source") -> JString("giraffe")))
    ))
  }
}

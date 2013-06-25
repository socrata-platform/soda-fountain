package com.socrata.soda.server

import org.scalatest.{Suite, BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io._
import com.rojoma.json.ast._

class SodaServerEndToEndTest extends IntegrationTest with BeforeAndAfterAll with IntegrationTestHelpers {

  val ct = System.currentTimeMillis
  val rowOpDataset = "soda-row-end-to-end-" + ct
  val datasetOpDataset = "soda-dataset-end-to-end-" + ct
  val resourceOpDataset = "soda-resource-functions-end-to-end-" + ct

  override def beforeAll = {
    removeFixtures
    val body = JObject(Map(
      "resource_name" -> JString(rowOpDataset),
      "name" -> JString("soda integration test upsert row"),
      "columns" -> JArray(Seq(
        column("text column", "col_text", Some("a text column"), "text"),
        column("num column", "col_num", Some("a number column"), "number")
      )),
      "row_identifier" -> JArray(Seq(JString("col_text")))
    ))
    val cRowOpD = dispatch("POST", "dataset", None, None, None,  Some(body))
    if (cRowOpD.getStatusCode != 200) throw new Exception( "create failed with " + cRowOpD.getStatusText + " " + cRowOpD.getResponseBody)

    //publish
    val pRowOpD = dispatch("PUT", "dataset-copy", Some(rowOpDataset), None, None, None)
    if (pRowOpD.getStatusCode != 200) throw new Exception( "publish failed with " + pRowOpD.getStatusText + " " + pRowOpD.getResponseBody)


    val cBody = JObject(Map(
      "resource_name" -> JString(resourceOpDataset),
      "name" -> JString("soda integration test upsert/replace/truncate/query dataset"),
      "row_identifier" -> JArray(Seq(JString("col_id"))),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))
    if (cResponse.getStatusCode != 200) throw new Exception( "create failed with " + cResponse.getStatusText + " " + cResponse.getResponseBody)

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceOpDataset), None, None, None)
    if (pResponse.getStatusCode != 200) throw new Exception( "publish failed with " + pResponse.getStatusText + " " + pResponse.getResponseBody)


  }

  protected def removeFixtures = {
    dispatch("DELETE", "dataset", Some(rowOpDataset), None, None, None)
    dispatch("DELETE", "dataset", Some(resourceOpDataset), None, None, None)
    dispatch("DELETE", "dataset", Some(datasetOpDataset), None, None, None)
  }

  override def afterAll = {
    removeFixtures
  }

  test("soda fountain can create/setSchema/getSchema/delete dataset"){

    val cBody = JObject(Map(
      "resource_name" -> JString(datasetOpDataset),
      "name" -> JString("soda integration test create/setSchema/getSchema/delete dataset"),
      "columns" -> JArray(Seq())
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))
    cResponse.getResponseBody must equal ("")
    cResponse.getStatusCode must equal (200)

    pendingUntilFixed{
      //set schema
      val sBody = JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
      val sResponse = dispatch("POST", "dataset", Some(datasetOpDataset), None, None,  Some(sBody))
      sResponse.getResponseBody must equal ("")
      sResponse.getStatusCode must equal (200)
    }

    //get schema
    val gResponse = dispatch("GET", "dataset", Some(datasetOpDataset), None, None,  None)
    gResponse.getResponseBody must equal ("{dataset schema}")
    gResponse.getStatusCode must equal (200)

    //delete
    val dResponse = dispatch("DELETE", "dataset", Some(datasetOpDataset), None, None,  None)
    dResponse.getResponseBody must equal ("")
    dResponse.getStatusCode must equal (200)

    //verify deleted
    val g2Response = dispatch("GET", "dataset", Some(datasetOpDataset), None, None,  None)
    g2Response.getResponseBody must equal ("g")
    g2Response.getStatusCode must equal (404)
  }

  test("soda fountain can upsert/replace/truncate/query dataset"){

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceOpDataset), None, None, None)
    val v1 = getVersionInSecondaryStore(resourceOpDataset)

    //upsert
    val uBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("row 1")))),
      JObject(Map(("col_id"->JNumber(2)), ("col_text"->JString("row 2"))))
    ))
    val uResponse = dispatch("POST", "resource", Some(resourceOpDataset), None, None,  Some(uBody))
    //uResponse.getResponseBody must equal ("{rows inserted}")
    uResponse.getStatusCode must equal (200)

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v1)
    val params = Map(("$query" -> "select *"))
    val qResponse = dispatch("GET", "resource", Some(resourceOpDataset), None, Some(params),  None)
    qResponse.getStatusCode must equal (200)
    jsonCompare(qResponse.getResponseBody,
      """[
        | { col_text : "row 1", col_id : 1.0 },
        | { col_text : "row 2", col_id : 2.0 }
        | ]""".stripMargin)

    //replace
    val v2 = getVersionInSecondaryStore(resourceOpDataset)
    val rBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(8)), ("col_text"->JString("row 8")))),
      JObject(Map(("col_id"->JNumber(9)), ("col_text"->JString("row 9"))))
    ))
    val rResponse = dispatch("PUT", "resource", Some(resourceOpDataset), None, None,  Some(rBody))
    rResponse.getStatusCode must equal (200)
    //rResponse.getResponseBody must equal ("[]")

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v2)
    val q2Response = dispatch("GET", "resource", Some(resourceOpDataset), None, None,  None)
    pendingUntilFixed{
      jsonCompare(q2Response.getResponseBody,
        """[
          | { col_text : "row 8", col_id : 8.0 },
          | { col_text : "row 9", col_id : 9.0 }
          | ]""".stripMargin)
    }
    q2Response.getStatusCode must equal (200)

    //truncate
    val v3 = getVersionInSecondaryStore(resourceOpDataset)
    val tResponse = dispatch("DELETE", "resource", Some(resourceOpDataset), None, None,  None)
    tResponse.getResponseBody must equal ("{rows deleted}")
    tResponse.getStatusCode must equal (200)

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v3)
    val q3Response = dispatch("GET", "resource", Some(resourceOpDataset), None, None,  None)
    jsonCompare(q3Response.getResponseBody, "[]")
    q3Response.getStatusCode must equal (200)
  }

  test("soda fountain can upsert/get/delete a row"){

    //upsert row
    val v1 = getVersionInSecondaryStore(rowOpDataset)
    val rowId = "rowZ"
    val urBody = JObject(Map(
      "col_text" -> JString(rowId),
      "col_num" -> JNumber(24601)
    ))
    val ur = dispatch("POST", "resource", Some(rowOpDataset), Some(rowId), None,  Some(urBody))
    ur.getStatusCode must equal (200)
    waitForSecondaryStoreUpdate(rowOpDataset, v1)

    //replace row
    val v2 = getVersionInSecondaryStore(rowOpDataset)
    val rrBody = JObject(Map(
      "col_text" -> JString(rowId),
      "col_num" -> JNumber(101010)
    ))
    val rr = dispatch("POST", "resource", Some(rowOpDataset), Some(rowId), None,  Some(rrBody))
    rr.getStatusCode must equal (200)
    waitForSecondaryStoreUpdate(rowOpDataset, v2)

    //get row
    val gr = dispatch("GET", "resource", Some(rowOpDataset), Some(rowId), None,  None)
    gr.getStatusCode must equal (200)
    jsonCompare(gr.getResponseBody, """[{ "col_num" : 101010.0, "col_text" : "rowZ" }]""")

    //delete row
    val v3 = getVersionInSecondaryStore(rowOpDataset)
    val dr = dispatch("DELETE", "resource", Some(rowOpDataset), Some(rowId), None,  None)
    dr.getStatusCode must equal (200)
    jsonCompare(dr.getResponseBody,
      """[{
        |"inserted":{},
        | "updated":{},
        | "deleted":{"1":"rowZ"},
        | "errors":{}
        | }] """.stripMargin)
    waitForSecondaryStoreUpdate(rowOpDataset, v3)

    //get row
    val gr2 = dispatch("GET", "resource", Some(rowOpDataset), Some(rowId), None,  None)
    pendingUntilFixed{
      gr2.getResponseBody must equal ("{verify row deleted}")
      gr2.getStatusCode must equal (404)
    }
  }
}

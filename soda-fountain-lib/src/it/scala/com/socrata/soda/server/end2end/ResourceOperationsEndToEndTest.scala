package com.socrata.soda.server.end2end

import com.socrata.soda.server._
import com.rojoma.json.ast._

class ResourceOperationsEndToEndTest extends IntegrationTest with IntegrationTestHelpers {

  val ct = System.currentTimeMillis
  val resourceOpDataset = "soda-resource-functions-end-to-end-" + ct

  test("soda fountain can upsert/replace/truncate/query dataset"){
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
    pendingUntilFixed{
      qResponse.getStatusCode must equal (200)
    }
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
    jsonCompare(q2Response.getResponseBody,
      """[
        | { col_text : "row 8", col_id : 8.0 },
        | { col_text : "row 9", col_id : 9.0 }
        | ]""".stripMargin)
    q2Response.getStatusCode must equal (200)

    //truncate
    val v3 = getVersionInSecondaryStore(resourceOpDataset)
    val tResponse = dispatch("DELETE", "resource", Some(resourceOpDataset), None, None,  None)
    //tResponse.getResponseBody must equal ("{rows deleted}")
    tResponse.getStatusCode must equal (200)

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v3)
    val q3Response = dispatch("GET", "resource", Some(resourceOpDataset), None, None,  None)
    jsonCompare(q3Response.getResponseBody, "[]")
    q3Response.getStatusCode must equal (200)
  }
}

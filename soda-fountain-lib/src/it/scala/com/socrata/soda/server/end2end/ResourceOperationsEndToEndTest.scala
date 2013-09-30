package com.socrata.soda.server.end2end

import com.socrata.soda.server._
import com.rojoma.json.ast._

class ResourceOperationsEndToEndTest extends SodaFountainIntegrationTest with IntegrationTestHelpers {

  val ct = System.currentTimeMillis
  val resourceOpDataset = "soda-resource-functions-end-to-end-" + ct

  test("soda fountain can upsert/replace/truncate/query dataset"){
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceOpDataset),
      "name" -> JString("soda integration test upsert/replace/truncate/query dataset"),
      "row_identifier" -> JString("col_id"),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val cResponse = sendWaitRead("POST", "dataset", None, None, None,  Some(cBody))
    if (cResponse.resultCode != 201) throw new Exception( "create failed with " + cResponse.resultCode + " " + readBody(cResponse))

    //publish
    val pResponse = sendWaitRead("PUT", "dataset-copy", Some(resourceOpDataset), None, None, None)
    if (pResponse.resultCode != 204) throw new Exception( "publish failed with " + pResponse.resultCode + " " + readBody(pResponse))
    //poke into secondary
    val gResponse = sendWaitRead("POST", "dataset-copy", Some(resourceOpDataset), Some(secondaryStore), None, None)
    val v1 = getVersionInSecondaryStore(resourceOpDataset)

    //upsert
    val uBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("row 1")))),
      JObject(Map(("col_id"->JNumber(2)), ("col_text"->JString("row 2"))))
    ))
    val uResponse = sendWaitRead("POST", "resource", Some(resourceOpDataset), None, None,  Some(uBody))
    //readBody(uResponse)must equal ("{rows inserted}")
    uResponse.resultCode must equal (200)

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v1)
    val params = Map(("$query" -> "select *"))
    val qResponse = sendWaitRead("GET", "resource", Some(resourceOpDataset), None, Some(params),  None)
    pendingUntilFixed{
      qResponse.resultCode must equal (200)
    }
    jsonCompare(readBody(qResponse),
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
    val rResponse = sendWaitRead("PUT", "resource", Some(resourceOpDataset), None, None,  Some(rBody))
    rResponse.resultCode must equal (200)
    //readBody(rResponse)must equal ("[]")

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v2)
    val q2Response = sendWaitRead("GET", "resource", Some(resourceOpDataset), None, None,  None)
    jsonCompare(readBody(q2Response),
      """[
        | { col_text : "row 8", col_id : 8.0 },
        | { col_text : "row 9", col_id : 9.0 }
        | ]""".stripMargin)
    q2Response.resultCode must equal (200)

    //truncate
    val v3 = getVersionInSecondaryStore(resourceOpDataset)
    val tResponse = sendWaitRead("DELETE", "resource", Some(resourceOpDataset), None, None,  None)
    //readBody(tResponse)must equal ("{rows deleted}")
    tResponse.resultCode must equal (200)

    //query
    waitForSecondaryStoreUpdate(resourceOpDataset, v3)
    val q3Response = sendWaitRead("GET", "resource", Some(resourceOpDataset), None, None,  None)
    jsonCompare(readBody(q3Response), "[]")
    q3Response.resultCode must equal (200)
  }
}

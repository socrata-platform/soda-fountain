package com.socrata.soda.server.end2end

import com.socrata.soda.server._
import com.rojoma.json.ast._

class RowOperationsEndToEndTest extends SodaFountainIntegrationTest with IntegrationTestHelpers {

  val ct = System.currentTimeMillis
  val rowOpDataset = "soda-row-end-to-end-" + ct

  test("soda fountain can upsert/get/delete a row"){

    val body = JObject(Map(
      "resource_name" -> JString(rowOpDataset),
      "name" -> JString("soda integration test upsert row"),
      "columns" -> JArray(Seq(
        column("text column", "col_text", Some("a text column"), "text"),
        column("num column", "col_num", Some("a number column"), "number")
      )),
      "row_identifier" -> JString("col_text")
    ))
    val cRowOpD = sendWaitRead("POST", "dataset", None, None, None,  Some(body))
    if (cRowOpD.resultCode != 201) throw new Exception( "create failed with " + cRowOpD.resultCode + " " + readBody(cRowOpD))

    //publish
    val pRowOpD = sendWaitRead("PUT", "dataset-copy", Some(rowOpDataset), None, None, None)
    if (pRowOpD.resultCode != 204) throw new Exception( "publish failed with " + pRowOpD.resultCode + " " + readBody(pRowOpD))

    val gResponse = sendWaitRead("POST", "dataset-copy", Some(rowOpDataset), Some(secondaryStore), None, None)
    val v1 = getVersionInSecondaryStore(rowOpDataset)

    //upsert row
    val rowId = "rowZ"
    val urBody = JObject(Map(
      "col_text" -> JString(rowId),
      "col_num" -> JNumber(24601)
    ))
    val ur = sendWaitRead("POST", "resource", Some(rowOpDataset), Some(rowId), None,  Some(urBody))
    ur.resultCode must equal (200)
    waitForSecondaryStoreUpdate(rowOpDataset, v1)

    //replace row
    val v2 = getVersionInSecondaryStore(rowOpDataset)
    val rrBody = JObject(Map(
      "col_text" -> JString(rowId),
      "col_num" -> JNumber(101010)
    ))
    val rr = sendWaitRead("POST", "resource", Some(rowOpDataset), Some(rowId), None,  Some(rrBody))
    rr.resultCode must equal (200)
    waitForSecondaryStoreUpdate(rowOpDataset, v2)

    //get row
    val gr = sendWaitRead("GET", "resource", Some(rowOpDataset), Some(rowId), None,  None)
    pendingUntilFixed{ // looks like race condition in ES
      gr.resultCode must equal (200)
      jsonCompare( readBody(gr) , """[{ "col_num" : 101010.0, "col_text" : "rowZ" }]""")
    }

    //delete row
    val v3 = getVersionInSecondaryStore(rowOpDataset)
    val dr = sendWaitRead("DELETE", "resource", Some(rowOpDataset), Some(rowId), None,  None)
    dr.resultCode must equal (200)
    jsonCompare(readBody(dr),
      """[{
        |"inserted":{},
        | "updated":{},
        | "deleted":{"1":"rowZ"},
        | "errors":{}
        | }] """.stripMargin)
    waitForSecondaryStoreUpdate(rowOpDataset, v3)

    //get row
    val gr2 = sendWaitRead("GET", "resource", Some(rowOpDataset), Some(rowId), None,  None)
    pendingUntilFixed{
      readBody(gr2) must equal ("{verify row deleted}")
      gr2.resultCode must equal (404)
    }
  }

}

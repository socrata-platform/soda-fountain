package com.socrata.soda.server.end2end

import com.socrata.soda.server._
import com.rojoma.json.ast._

class DatasetOperationsEndToEndTest extends IntegrationTest with IntegrationTestHelpers {

  val ct = System.currentTimeMillis
  val datasetOpDataset = "soda-dataset-end-to-end-" + ct

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

}

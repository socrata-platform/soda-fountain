package com.socrata.soda.server.end2end

import com.socrata.soda.server._
import com.rojoma.json.ast._
import com.socrata.soda.server.wiremodels.DatasetSpec
import com.rojoma.json.codec.JsonCodec

class DatasetOperationsEndToEndTest extends SodaFountainIntegrationTest with IntegrationTestHelpers {

  val ct = System.currentTimeMillis
  val datasetOpDataset = "soda-dataset-end-to-end-" + ct

  test("soda fountain can create/setSchema/getSchema/delete dataset"){

    val cBody = JObject(Map(
      "resource_name" -> JString(datasetOpDataset),
      "name" -> JString("soda integration test create/setSchema/getSchema/delete dataset"),
      "columns" -> JArray(Seq())
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))
    JsonCodec[DatasetSpec].decode(cResponse.body) match {
      case Some(spec) => {}
      case _ => fail("did not receive a schema in response to the dataset create request")
    }
    cResponse.resultCode must equal (201)

    pendingUntilFixed{
      //set schema
      val sBody = JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
      val sResponse = dispatch("POST", "dataset", Some(datasetOpDataset), None, None,  Some(sBody))
      readBody(sResponse)must equal ("")
      sResponse.resultCode must equal (200)
    }

    //get schema
    val gResponse = dispatch("GET", "dataset", Some(datasetOpDataset), None, None,  None)
    readBody(gResponse)must equal ("{dataset schema}")
    gResponse.resultCode must equal (200)

    //delete
    val dResponse = dispatch("DELETE", "dataset", Some(datasetOpDataset), None, None,  None)
    readBody(dResponse)must equal ("")
    dResponse.resultCode must equal (200)

    //verify deleted
    val g2Response = dispatch("GET", "dataset", Some(datasetOpDataset), None, None,  None)
    readBody(g2Response)must equal ("g")
    g2Response.resultCode must equal (404)
  }

}

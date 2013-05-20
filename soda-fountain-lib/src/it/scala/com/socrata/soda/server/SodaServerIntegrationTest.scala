package com.socrata.soda.server

import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil

class SodaServerIntegrationTest extends IntegrationTest {

  test("update request malformed json returns error response"){
    val response = dispatch("POST", "resource", Option("testDataset"), None, Some(JString("this is not json")))
    response.getResponseBody.length must be > (0)
    response.getStatusCode must equal (415)
  }

  test("update request with unexpected format json returns error response"){
    val response = dispatch("POST", "resource", Option("testDataset"), None, Some(JArray(Array(JString("this is an array"), JString("why would you post an array?")))))
    response.getStatusCode must equal (400)
  }

  test("soda fountain getSchema"){
    val resourceName = "soda-get-schema"
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test getSchema"),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, Some(cBody))
    cResponse.getResponseBody must equal ("")
    cResponse.getStatusCode must equal (200)

    //get schema
    val gResponse = dispatch("GET", "dataset", Some(resourceName), None, None)
    gResponse.getStatusCode must equal (200)
    val m = JsonUtil.parseJson[Map[String,JValue]](gResponse.getResponseBody)
    m match {
      case Some(map) => {
        map.get("hash").getClass must be (classOf[Some[String]])
        map.get("pk").getClass must be (classOf[Some[String]])
        map.get("schema").getClass must be (classOf[Some[JObject]])
      }
      case None => fail("did not receive schema from soda server")
    }
  }

  test("soda fountain upsert"){

    val resourceName = "soda-upsert"
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test upsert"),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, Some(cBody))
    cResponse.getResponseBody must equal ("")
    cResponse.getStatusCode must equal (200)

    //upsert
    val uBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("row 1")))),
      JObject(Map(("col_id"->JNumber(2)), ("col_does_not_exist"->JString("row 2")))),
      JObject(Map(("col_id"->JNumber(3)), ("col_text"->JString("row 3"))))
    ))
    val uResponse = dispatch("POST", "resource", Some(resourceName), None, Some(uBody))
    uResponse.getResponseBody must equal ("{rows inserted}")
    uResponse.getStatusCode must equal (200)
  }

}

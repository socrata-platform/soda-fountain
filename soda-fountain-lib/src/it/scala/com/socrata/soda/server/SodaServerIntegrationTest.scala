package com.socrata.soda.server

import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import org.scalatest._

trait SodaServerIntegrationTestFixture extends BeforeAndAfterAll with IntegrationTestHelpers { this: Suite =>

  val resourceName = "soda-int-test"

  override def beforeAll = {
    println("creating dataset for integration tests")
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test"),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))
    //assert(cResponse.getStatusCode == 200)

    val uBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("row 1")))),
      JObject(Map(("col_id"->JNumber(2)), ("col_does_not_exist"->JString("row 2")))),
      JObject(Map(("col_id"->JNumber(3)), ("col_text"->JString("row 3"))))
    ))
    val uResponse = dispatch("POST", "resource", Some(resourceName), None, None,  Some(uBody))
    //assert(uResponse.getStatusCode == 200)
  }

  override def afterAll = {
    println("running the afterAll block")
  }
}

class SodaServerIntegrationTest extends IntegrationTest with SodaServerIntegrationTestFixture {

  test("update request malformed json returns error response"){
    val response = dispatch("POST", "resource", Option(resourceName), None, None,  Some(JString("this is not json")))
    response.getResponseBody.length must be > (0)
    response.getStatusCode must equal (415)
  }

  test("update request with unexpected format json returns error response"){
    val response = dispatch("POST", "resource", Option(resourceName), None, None,  Some(JArray(Array(JString("this is an array"), JString("why would you post an array?")))))
    response.getStatusCode must equal (400)
  }

  test("soda fountain getSchema"){
    //get schema
    val gResponse = dispatch("GET", "dataset", Some(resourceName), None, None,  None)
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
    //upsert
    val uBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("upserted row 1")))),
      JObject(Map(("col_id"->JNumber(2)), ("col_does_not_exist"->JString("row 2")))),
      JObject(Map(("col_id"->JNumber(3)), ("col_text"->JString("upserted row 3"))))
    ))
    val uResponse = dispatch("POST", "resource", Some(resourceName), None, None,  Some(uBody))
    //uResponse.getResponseBody must equal ("{rows inserted}")
    uResponse.getStatusCode must equal (200)
  }

  test("soda fountain query") {
    //query
    val params = Map(("$query" -> "select *"))
    val qResponse = dispatch("GET", "resource", Some(resourceName), None, Some(params),  None)
    qResponse.getResponseBody must equal ("[{ \"col_text\" : \"row 3\", \"col_id\" : 3.0 },{ \"col_text\" : \"row 1\", \"col_id\" : 1.0 },{ \"col_id\" : 2.0}]")
    qResponse.getStatusCode must equal (200)
  }

}

package com.socrata.soda.server

import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import org.scalatest._

trait RowServiceIntegrationTestFixture extends BeforeAndAfterAll with IntegrationTestHelpers { this: Suite =>

  val resourceName = "soda-int-test-row-service"

  override def beforeAll = {
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test for row service"),
      "row_identifier" -> JArray(Seq(JString("col_id"))),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceName), None, None, None)
    val v = getVersionInSecondaryStore(resourceName)

    val uBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("row 1")))),
      JObject(Map(("col_id"->JNumber(2)), ("col_text"->JString("row 2")))),
      JObject(Map(("col_id"->JNumber(3)), ("col_text"->JString("row 3")))),
      JObject(Map(("col_id"->JNumber(4)), ("col_text"->JString("row 4 at " + System.currentTimeMillis.toString)))) //current time ensures data version increments
    ))
    val uResponse = dispatch("POST", "resource", Some(resourceName), None, None,  Some(uBody))
    assert(uResponse.getStatusCode == 200)

    waitForSecondaryStoreUpdate(resourceName, v)
  }

  override def afterAll = {
  }
}

class RowServiceIntegrationTest extends IntegrationTest with RowServiceIntegrationTestFixture {

  test("soda fountain row service upsert"){
    val uBody =  JObject(Map(("col_id"->JNumber(3)), ("col_text"->JString("upserted row 3"))))
    val uResponse = dispatch("POST", "resource", Some(resourceName), Some("3"), None, Some(uBody))
    assert(uResponse.getStatusCode === 200, uResponse.getResponseBody)
  }

  test("soda fountain row service get"){
    val uResponse = dispatch("GET", "resource", Some(resourceName), Some("2"), None, None)
    assert(uResponse.getStatusCode === 200, uResponse.getResponseBody)
    jsonCompare(uResponse.getResponseBody, """{ col_text:'row 2', col_id:2.0}""")
  }

  test("soda fountain row service 404"){
    val uResponse = dispatch("GET", "resource", Some(resourceName), Some("787878"), None, None)
    assert(uResponse.getStatusCode === 404, uResponse.getResponseBody)
    jsonCompare(uResponse.getResponseBody, """{"message":"row not found","errorCode":"row.not.found"}""")
  }

  test("soda fountain row service remove"){
    val v = getVersionInSecondaryStore(resourceName)
    val uResponse = dispatch("DELETE", "resource", Some(resourceName), Some("1"), None, None)
    assert(uResponse.getStatusCode === 204, uResponse.getResponseBody)
    assert(uResponse.getResponseBody == "", uResponse.getResponseBody)
    waitForSecondaryStoreUpdate(resourceName, v)
  }

}

package com.socrata.soda.server

import org.scalatest.{BeforeAndAfterAll, Suite}
import com.rojoma.json.ast._

trait SoQLTypeIntegrationTestFixture extends BeforeAndAfterAll with IntegrationTestHelpers { this: Suite =>

  val resourceName = "soql-type-integration-test" + System.currentTimeMillis.toString

  override def beforeAll = {

    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda fountain soql type integration test"),
      "row_identifier" -> JArray(Seq(JString("test_id"))),
      "columns" -> JArray(Seq(
        column("the ID column", "test_id", Some("this is the ID column"), "number"),
        column("the ID column", "test_double", Some("type double"), "double"),
        column("a text column", "test_text", Some("this is a text column"), "text"),
        column("a boolean column", "test_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceName), None, None, None)

    /*
    val v = getVersionInSecondaryStore(resourceName)
    val uBody = JArray(Seq(
      JObject(Map(("test_id"->JNumber(1)))),
      //JObject(Map(("test_id"->JNumber(2)), ("test_double"-> JNumber(0.33333)))),
      JObject(Map(("test_id"->JNumber(3)), ("test_text"->JString("row 3")))),
      JObject(Map(("test_id"->JNumber(4)), ("test_text"->JString("row 4"))))
    ))
    val uResponse = dispatch("PUT", "resource", Some(resourceName), None, None,  Some(uBody))
    assert(uResponse.getStatusCode == 200)

    waitForSecondaryStoreUpdate(resourceName, v)
    */

  }
}

class SoQLTypeIntegrationTest extends IntegrationTest with SoQLTypeIntegrationTestFixture  {

  test("upsert type double"){
    val v = getVersionInSecondaryStore(resourceName)
    val uBody = JArray(Seq(
      JObject(Map(("test_id"->JNumber(100)), ("test_double"-> JNumber(0.333))))
    ))
    val uResponse = dispatch("PUT", "resource", Some(resourceName), None, None,  Some(uBody))
    assert(uResponse.getStatusCode == 200, uResponse.getResponseBody)

    val params = Map(("$query" -> "select * where test_double = 0.333"))
    val qResponse = dispatch("GET", "resource", Some(resourceName), None, Some(params),  None)
    pendingUntilFixed{
      jsonCompare(qResponse.getResponseBody, """[{test_double:0.333, col_id: 100.0}]""".stripMargin )
      qResponse.getStatusCode must equal (200)
    }
  }
}


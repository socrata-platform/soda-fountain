package com.socrata.soda.server

import org.scalatest.{BeforeAndAfterAll, Suite}
import com.rojoma.json.ast._

trait SoQLTypeIntegrationTestFixture extends BeforeAndAfterAll with IntegrationTestHelpers { this: Suite =>

  //TODO: when these tests are stable, the rn can be refactored to be stable, and the fixture can simply truncate and replace rows, to reduce dataset churn.
  val resourceName = "soql-type-integration-test" + System.currentTimeMillis.toString

  override def beforeAll = {

    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda fountain soql type integration test"),
      "row_identifier" -> JArray(Seq(JString("test_id"))),
      "columns" -> JArray(Seq(
        column("the ID column", "test_id", Some("this is the ID column"), "number"),
        column("the double type column",  "test_double", Some("type double"), "double"),
        column("the money type column",   "test_money", Some("type money"), "money"),
        column("a text column",           "test_text", Some("this is a text column"), "text"),
        column("a boolean column", "test_bool", None, "boolean")
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceName), None, None, None)
  }
}

class SoQLTypeIntegrationTest extends IntegrationTest with SoQLTypeIntegrationTestFixture  {

  def testType( row: Map[String, JValue], query: String, expectedResult: String) = {
    val v = getVersionInSecondaryStore(resourceName)
    val uBody = JArray(Seq( JObject(row) ))
    val uResponse = dispatch("PUT", "resource", Some(resourceName), None, None,  Some(uBody))
    assert(uResponse.getStatusCode == 200, uResponse.getResponseBody)

    waitForSecondaryStoreUpdate(resourceName, v)
    val params = Map(("$query" -> query))
    val qResponse = dispatch("GET", "resource", Some(resourceName), None, Some(params),  None)
    jsonCompare(qResponse.getResponseBody, expectedResult )
    qResponse.getStatusCode must equal (200)
  }

  test("upsert type double"){ testType(Map(("test_id"->JNumber(100)), ("test_double"-> JNumber(0.333))), "select * where test_double = 0.333", """[{test_double:0.333, col_id: 100.0}]""".stripMargin) }
  test("upsert type money") { testType(Map(("test_id"->JNumber(101)), ("test_money" -> JNumber(0.55))),  "select * where test_money  = 0.55",  """[{test_money:0.333,  col_id: 101.0}]""".stripMargin) }
}


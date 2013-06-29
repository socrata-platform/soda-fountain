package com.socrata.soda.server

import org.scalatest.{Suite, BeforeAndAfterAll}
import com.rojoma.json.ast._

object ColumnServiceIntegrationTest {
  val rn = "soda-column-service-int-test-2"
}

trait ColumnServiceIntegrationTestFixture extends BeforeAndAfterAll with IntegrationTestHelpers { this: Suite =>


  def fixtureCreate(postBody: JObject) = {
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(postBody))
  }

  override def beforeAll = {
    val c1Body = JObject(Map(
      "resource_name" -> JString(ColumnServiceIntegrationTest.rn),
      "name" -> JString("soda column service integration test"),
      "row_identifier" -> JArray(Seq(JString("col_id"))),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))

    fixtureCreate(c1Body)

    //publish
    //val pResponse = dispatch("PUT", "dataset-copy", Some(ColumnServiceIntegrationTest.rn), None, None, None)
    //val v = getVersionInSecondaryStore(ColumnServiceIntegrationTest.rn)

    //upsert values.  The current time in the last row will cause the data version to increment.
    val uBody = JArray(Seq( JObject(Map(("col_id"->JNumber(1)), ("col_text"->JString("row 1")))) ))
    val uResponse = dispatch("POST", "resource", Some(ColumnServiceIntegrationTest.rn), None, None,  Some(uBody))
    if (uResponse.getStatusCode != 200){throw new Exception("fixture upsert unsuccessful")}

    //waitForSecondaryStoreUpdate(ColumnServiceIntegrationTest.rn, v)
  }

  override def afterAll = {}
}

class ColumnServiceIntegrationTest extends IntegrationTest with ColumnServiceIntegrationTestFixture {

  test("column service getSchema") {
    val gResponse = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some("col_id"), None,  None)
    gResponse.getStatusCode must equal (200)
    val body = gResponse.getResponseBody
    assert(body.contains(""""field_name" : "col_id""""))
    assert(body.contains(""""datatype" : "number""""))
  }
  test("column service - add/drop column") {
    val id = "new_col"
    val newCol = column("new column", id, Some("new col for add drop col int test"), "text")

    //delete column (in case it's left over by accident)
    val d1Response = dispatch("DELETE", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)

    //verify it's not there
    val g1Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)
    g1Response.getStatusCode must equal (404)

    //add column
    val pResponse = dispatch("POST", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  Some(newCol))
    pResponse.getStatusCode must equal (200)

    //verify it's been created
    val g2Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)
    g2Response.getStatusCode must equal (200)
    jsonCompare(g2Response.getResponseBody, "{ verify:'column schema'}")

    //delete column
    val d2Response = dispatch("DELETE", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)
    d2Response.getStatusCode must equal (204)

    //verify it's been deleted
    val g3Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)
    g3Response.getStatusCode must equal (404)
  }

  test("column service update - rename column") {
    val id = "name_rename"
    val id2 = "name_rename_renamed"
    val newCol = column("column to test renaming", id, Some("new col for add drop col int test"), "text")
    val updatedCol = column("renamed column to test renaming", id2, Some("new col for add drop col int test"), "text")

    //delete column (in case it's left over by accident)
    val d1Response = dispatch("DELETE", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)

    //verify it's not there
    val g1Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)
    g1Response.getStatusCode must equal (404)

    //add column
    val pResponse = dispatch("POST", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  Some(newCol))
    pResponse.getStatusCode must equal (200)

    //verify it's been created
    val g2Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  None)
    g2Response.getStatusCode must equal (200)
    assert(g2Response.getResponseBody.contains(""""field_name" : "name_rename""""))

    //update column
    val p2Response = dispatch("POST", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id), None,  Some(updatedCol))
    p2Response.getStatusCode must equal (200)

    //verify updated column exists
    val g3Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id2), None,  None)
    g3Response.getStatusCode must equal (200)
    assert(g3Response.getResponseBody.contains(""""field_name" : "name_rename_renamed""""))

    //verify old column doesn't exist
    val g4Response = dispatch("GET", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id2), None,  None)
    g4Response.getStatusCode must equal (404)

    //delete column
    val d2Response = dispatch("DELETE", "dataset", Some(ColumnServiceIntegrationTest.rn), Some(id2), None,  None)
    d2Response.getStatusCode must equal (204)
  }

  test("column service update - change column type") (pending)
}

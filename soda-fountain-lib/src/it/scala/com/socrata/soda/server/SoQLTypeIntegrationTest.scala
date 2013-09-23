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
      "row_identifier" -> JString("test_id"),
      "columns" -> JArray(Seq(
        column("the double type column",  "test_double",                    None, "double"              ),
        column("the money type column",   "test_money",                     None, "money"               ),
        column("a text column",           "test_text",                      None, "text"                ),
        column("fixed_typestamp column",  "test_fixed_timestamp",           None, "fixed_timestamp"     ),
        column("date column",             "test_date",                      None, "date"                ),
        column("time column",             "test_time",                      None, "time"                ),
        column("object column",           "test_object",                    None, "object"              ),
        column("array column",            "test_array",                     None, "array"               ),
        column("location column",         "test_location",                  None, "location"            ),
        //column("json column",             "test_json",                      None, "json"                ),
        column("a boolean column",        "test_boolean",                   None, "boolean"             ),
        column("floating_timestamp col",  "test_floating_timestamp",        None, "floating_timestamp"  ),
        column("the ID column",           "test_id",                        None, "number"              )
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceName), None, None, None)
  }
}

class SoQLTypeIntegrationTest extends SodaFountainIntegrationTest with SoQLTypeIntegrationTestFixture  {

  def testType( row: Map[String, JValue], query: String, expectedResult: String) = {
    val v = getVersionInSecondaryStore(resourceName)
    val uBody = JArray(Seq( JObject(row) ))
    val uResponse = dispatch("POST", "resource", Some(resourceName), None, None,  Some(uBody))
    assert(uResponse.resultCode == 200, readBody(uResponse))
    //TODO: remove the following line when ES can guarantee consistency.
    Thread.sleep(3000)  //arg. wait out the race condition until we can guarantee consistency.
    waitForSecondaryStoreUpdate(resourceName, v)
    val params = Map(("$query" -> query))
    val qResponse = dispatch("GET", "resource", Some(resourceName), None, Some(params),  None)
    assert(qResponse.resultCode == 200, readBody(qResponse))
    jsonCompare(readBody(qResponse), expectedResult )
  }

  //test("upsert/query type json    ") { testType(Map(("test_id"->JNumber(110)), ("test_json    " -> )),  "select * where test_json    = ",  """[{test_json    :,  test_id:110.0 }]""".stripMargin) }
  test("upsert/query type number") { testType(Map(("test_id"->JNumber(100))),"select * where test_id = 100",  """[{test_id:100.0}]""".stripMargin)}

  test("upsert/query type double") {
    /*
     TODO:fix https://redmine.socrata.com/issues/11668
     "errorCode":"query.soql.type-mismatch","data":{"data":{"function":"op$=","type":"*number","dataset":"alpha.551","position":{"row":1,"column":30,"line":"select * where test_double = 0.333}}}}
     */
    pendingUntilFixed{
      testType(Map(("test_id"->JNumber(101)), ("test_double" -> JNumber(0.333))),  "select * where test_double = 0.333",  """[{test_double:0.333,  test_id:101.0 }]""".stripMargin)
    }
  }

  test("upsert/query type money") {
    /*
    TODO: fix https://redmine.socrata.com/issues/11669
    exception from soql-es-adapter: com.socrata.es.soql.NotImplementedException: Expression not implemented op$to_money(1.59 :: number) :: money
     */
    pendingUntilFixed{
      testType(Map(("test_id"->JNumber(102)), ("test_money" -> JNumber(1.59))),  "select * where test_money = 1.59",  """[{test_money: 1.59,  test_id:102.0 }]""".stripMargin)
    }
  }

  test("upsert/query type text") { testType(Map(("test_id"->JNumber(103)), ("test_text" -> JString("eastlake"))),  "select * where test_text = 'eastlake'",  """[{test_id:103.0, test_text: 'eastlake'}]""".stripMargin) }
  test("upsert/query type object") { testType(Map(("test_id"->JNumber(107)), ("test_object" -> JObject(Map(("firstname"->JString("daniel")),("lastname"-> JString("rathbone")))))),  "select * where test_object.firstname::text = 'daniel'",  """[{test_object:{firstname:'daniel', lastname:'rathbone'},  test_id:107.0 }]""".stripMargin) }

  test("upsert/query type array") {
    /*
    TODO: fix https://redmine.socrata.com/issues/11670
    com.socrata.es.soql.NotImplementedException: Expression not implemented cast$boolean(op$[](test_array :: array,0 :: number) :: json) :: boolean (query: select * where test_array0::boolean = true )
     */
    pendingUntilFixed{
      testType(Map(("test_id"->JNumber(108)), ("test_array" -> JArray(Seq(JBoolean(true), JNumber(99))))),  "select * where test_array[0]::boolean = true",  """[{test_array :[true, 99.0],  test_id:108.0 }]""".stripMargin)
    }
  }

  test("upsert/query type location") {
    /* TODO: fix https://redmine.socrata.com/issues/11671 */
    pendingUntilFixed{
      testType(Map(("test_id"->JNumber(109)), ("test_location" -> JArray(Seq(JNumber(45.0), JNumber(39.0), JNull)))),  "select * where test_location.latitude = 45.0",  """[{test_location:[45.0, 39.0, null],  test_id:109.0 }]""".stripMargin)
    }
  }

  test("upsert/query type boolean") { testType(Map(("test_id"->JNumber(111)), ("test_boolean" -> JBoolean(true))),  "select * where test_boolean = true",  """[{test_id:111.0, test_boolean:true}]""".stripMargin) }

  test("upsert/query type date") {
    //TODO: fix https://redmine.socrata.com/issues/11672
    pendingUntilFixed{
      testType(Map(("test_id"->JNumber(105)), ("test_date" -> JString("2013-07-15"))),  "select * where test_date    = '2013-07-15'",  """[{test_date    :'2013-07-15',  test_id:105.0 }]""".stripMargin)
    }
  }

  test("upsert/query type time") {
    //TODO: fix https://redmine.socrata.com/issues/11672
    pendingUntilFixed{
      testType(Map(("test_id"->JNumber(106)), ("test_time" -> JString("02:10:49.123"))),  "select * where test_time    = '02:10:49.123'",  """[{test_time    :'02:10:49.123',  test_id:106.0 }]""".stripMargin)
    }
  }

  test("upsert/query type fixed_timestamp   ") {
    testType(
      Map(("test_id"->JNumber(104)), ("test_fixed_timestamp" -> JString("2013-07-15T02:10:49.123Z"))),
      "select * where test_fixed_timestamp = '2013-07-15T02:10:49.123Z'",
      """[{test_fixed_timestamp :'2013-07-15T02:10:49.123Z',  test_id:104.0 }]""".stripMargin
    )
  }

  test("upsert/query type floating_timestamp") {
    testType(
      Map(("test_id"->JNumber(112)), ("test_floating_timestamp" -> JString("2013-07-15T02:10:49.123"))),
      "select * where test_floating_timestamp = '2013-07-15T02:10:49.123'",
      """[{test_floating_timestamp:'2013-07-15T02:10:49.123',  test_id:112.0 }]""".stripMargin
    )
  }
}

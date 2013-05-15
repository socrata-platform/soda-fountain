package com.socrata.soda.server

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io._
import com.rojoma.json.ast._


class SodaServerIntegrationTest extends IntegrationTest {

  val hostname: String = "localhost:8080"

  def column(name: String, fieldName: String, oDesc: Option[String], datatype: String): JObject = {
    val base = Map(
      "name" -> JString(name),
      "field_name" -> JString(fieldName),
      "datatype" -> JString(datatype)
    )
    val map = oDesc match { case Some(desc) => base + ("description" -> JString(desc)); case None => base }
    JObject(map)
  }

  def dispatch(method: String, service:String, part1o: Option[String], part2o: Option[String], oBody: Option[JValue]) = {
    val url = host(hostname) / service
    val request = part1o.foldLeft(url){ (url1, part1) =>
      part2o.foldLeft( url1 / part1 ) ( (url2, part2) => url2 / part2)
    }
    request.setMethod(method).addHeader("Content-Type", "application/json;charset=utf-8")
    oBody match {
      case Some(body) => request.setBody(body.toString)
      case None => request
    }
    val response = for (r <- Http(request).either.right) yield r
    response() match {
      case Right(response) => response
      case Left(thr) => throw thr
    }
  }

  test("update request malformed json returns error response"){
    val response = dispatch("POST", "resource", Option("testDataset"), None, Some(JString("this is not json")))
    response.getResponseBody.length must be > (0)
    response.getStatusCode must equal (415)
  }

  test("update request with unexpected format json returns error response"){
    val response = dispatch("POST", "resource", Option("testDataset"), None, Some(JArray(Array(JString("this is an array"), JString("why would you post an array?")))))
    response.getStatusCode must equal (400)
  }

  test("soda fountain can create/setSchema/getSchema/delete dataset"){
    val resourceName = "soda-int-dataset"
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test create/setSchema/getSchema/delete dataset"),
      "columns" -> JArray(Seq())
    ))
    val cResponse = dispatch("POST", "dataset", None, None, Some(cBody))
    cResponse.getResponseBody must equal ("")
    cResponse.getStatusCode must equal (200)

    //set schema
    val sBody = JObject(Map(
      "row_identifier" -> JArray(Seq(JString("col_id"))),
      "columns" -> JArray(Seq(
        column("the ID column", "col_id", Some("this is the ID column"), "number"),
        column("a text column", "col_text", Some("this is a text column"), "text"),
        column("a boolean column", "col_bool", None, "boolean")
      ))
    ))
    val sResponse = dispatch("POST", "dataset", Some(resourceName), None, Some(sBody))
    sResponse.getResponseBody must equal ("")
    sResponse.getStatusCode must equal (200)

    //get schema
    val gResponse = dispatch("GET", "dataset", Some(resourceName), None, None)
    gResponse.getResponseBody must equal ("{dataset schema}")
    gResponse.getStatusCode must equal (200)

    //delete
    val dResponse = dispatch("DELETE", "dataset", Some(resourceName), None, None)
    dResponse.getResponseBody must equal ("")
    dResponse.getStatusCode must equal (200)

    //verify deleted
    val g2Response = dispatch("GET", "dataset", Some(resourceName), None, None)
    g2Response.getResponseBody must equal ("g")
    g2Response.getStatusCode must equal (404)
  }

  test("soda fountain can upsert/replace/truncate/query dataset"){
    val resourceName = "soda-int-resource-functions"
    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test upsert/replace/truncate/query dataset"),
      "row_identifier" -> JArray(Seq(JString("col_id"))),
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
      JObject(Map(("col_id"->JNumber(2)), ("col_text"->JString("row 2"))))
    ))
    val sResponse = dispatch("POST", "dataset", Some(resourceName), None, Some(uBody))
    sResponse.getResponseBody must equal ("{rows inserted}")
    sResponse.getStatusCode must equal (200)

    //query
    val qResponse = dispatch("GET", "dataset", Some(resourceName), None, None)
    qResponse.getResponseBody must equal ("{rows 1 and 2}")
    qResponse.getStatusCode must equal (200)

    //replace
    val rBody = JArray(Seq(
      JObject(Map(("col_id"->JNumber(8)), ("col_text"->JString("row 8")))),
      JObject(Map(("col_id"->JNumber(9)), ("col_text"->JString("row 9"))))
    ))
    val rResponse = dispatch("PUT", "dataset", Some(resourceName), None, Some(rBody))
    rResponse.getResponseBody must equal ("{rows put}")
    rResponse.getStatusCode must equal (200)

    //query
    val q2Response = dispatch("GET", "dataset", Some(resourceName), None, None)
    q2Response.getResponseBody must equal ("{rows 8 and 9}")
    q2Response.getStatusCode must equal (200)

    //truncate
    val tResponse = dispatch("DELETE", "dataset", Some(resourceName), None, None)
    tResponse.getResponseBody must equal ("{rows deleted}")
    tResponse.getStatusCode must equal (200)

    //query
    val q3Response = dispatch("GET", "dataset", Some(resourceName), None, None)
    q3Response.getResponseBody must equal ("{all rows should be truncated}")
    q3Response.getStatusCode must equal (200)
  }

  test("soda fountain can upsert/get/delete a row"){
    val resourceName = "soda-int-row"
    val body = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda integration test upsert row"),
      "columns" -> JArray(Seq(
        column("text column", "col_text", Some("a text column"), "text"),
        column("num column", "col_num", Some("a number column"), "number")
      )),
      "row_identifier" -> JString("col_text")
    ))
    val d = dispatch("POST", "dataset", None, None, Some(body))

    d.getResponseBody must equal ("")
    d.getStatusCode must equal (200)

    //upsert row
    val rowId = "rowZ"
    val urBody = JObject(Map(
    "col_text" -> JString(rowId),
    "col_num" -> JNumber(24601)
    ))
    val ur = dispatch("POST", "resource", Some(resourceName), Some(rowId), Some(urBody))
    ur.getStatusCode must equal (200)

    //replace row
    val rrBody = JObject(Map(
    "col_text" -> JString(rowId),
    "col_num" -> JNumber(101010)
    ))
    val rr = dispatch("POST", "resource", Some(resourceName), Some(rowId), Some(rrBody))
    rr.getStatusCode must equal (200)

    //get row
    val gr = dispatch("GET", "resource", Some(resourceName), Some(rowId), None)
    gr.getStatusCode must equal (200)
    gr.getResponseBody must equal ("{row get response}")

    //delete row
    val dr = dispatch("DELETE", "resource", Some(resourceName), Some(rowId), None)
    dr.getStatusCode must equal (200)
    dr.getResponseBody must equal ("{row delete response}")

    //get row
    val gr2 = dispatch("GET", "resource", Some(resourceName), Some(rowId), None)
    gr2.getStatusCode must equal (404)
    gr2.getResponseBody must equal ("{verify row deleted}")

  }
}

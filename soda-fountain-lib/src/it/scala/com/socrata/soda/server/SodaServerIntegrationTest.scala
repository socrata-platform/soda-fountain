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
    request.setMethod(method).addHeader("Content-Type", "application/json")
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

  test("soda fountain can create dataset"){
    val body = JObject(Map(
      "resource_name" -> JString("soda-int-create"),
      "name" -> JString("soda integration test create dataset"),
      "columns" -> JArray(Seq())
    ))
    val response = dispatch("POST", "dataset", None, None, Some(body))
    response.getResponseBody must equal ("")
    response.getStatusCode must equal (200)
  }

  test("soda fountain can insert/update/get/delete a row"){
    val resourceName = "soda-int-upsert-row"
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

    val rowId = "rowZ"
    val urBody = JObject(Map(
    "col_text" -> JString(rowId),
    "col_num" -> JNumber(24601)
    ))
    val ur = dispatch("POST", "resource", Some(resourceName), Some(rowId), Some(urBody))
    ur.getStatusCode must equal (200)

    val rrBody = JObject(Map(
    "col_text" -> JString(rowId),
    "col_num" -> JNumber(101010)
    ))
    val rr = dispatch("POST", "resource", Some(resourceName), Some(rowId), Some(rrBody))
    rr.getStatusCode must equal (200)

    val gr = dispatch("GET", "resource", Some(resourceName), Some(rowId), None)
    gr.getStatusCode must equal (200)
    gr.getResponseBody must equal ("{row get response}")

    val dr = dispatch("DELETE", "resource", Some(resourceName), Some(rowId), None)
    dr.getStatusCode must equal (200)
    dr.getResponseBody must equal ("{row delete response}")

    val gr2 = dispatch("GET", "resource", Some(resourceName), Some(rowId), None)
    gr2.getStatusCode must equal (404)
    gr2.getResponseBody must equal ("{verify row deleted}")

  }
}

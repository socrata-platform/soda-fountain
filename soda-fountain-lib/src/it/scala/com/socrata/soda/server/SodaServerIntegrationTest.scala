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

  def post(service:String, part1o: Option[String], part2o: Option[String], body: JValue) = {
    val url = host(hostname) / service
    val request = part1o.foldLeft(url){ (url1, part1) =>
      part2o.foldLeft( url1 / part1 ) ( (url2, part2) => url2 / part2)
    }
    request.POST.
      addHeader("Content-Type", "application/json").
      setBody(body.toString)
    Http(request)
  }

  test("update request malformed json returns error response"){
    def result = for (r <- post("resource", Option("testDataset"), None, JString("this is not json")).either.right) yield r
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => {
        response.getResponseBody.length must be > (0)
        response.getStatusCode must equal (415)
      }
    }
  }

  test("update request with unexpected format json returns error response"){
    def result = for (r <- post("resource", Option("testDataset"), None, JArray(Array(JString("this is an array"), JString("why would you post an array?")))).either.right) yield r
    result() match {
      case Right(response) => response.getStatusCode must equal (400)
      case Left(exc) => fail(exc.getMessage)
    }
  }

  test("soda fountain can create dataset"){
    val body = JObject(Map(
      "resource_name" -> JString("soda-int-create"),
      "name" -> JString("soda integration test create dataset"),
      "columns" -> JArray(Seq())
    ))
    val r = for { r <- post("dataset", None, None, body).either.right } yield r
    r() match {
      case Right(response) => {
        response.getResponseBody must equal ("")
        response.getStatusCode must equal (200)
      }
      case Left(thr) => throw thr
    }
  }

  test("soda fountain can upsert a row"){
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
    val r = for { r <- post("dataset", None, None, body).either.right } yield r
    r() match {
      case Right(response) => {
        response.getResponseBody must equal ("")
        response.getStatusCode must equal (200)

        val rowId = "rowZ"
        val rowBody = JObject(Map(
          "col_text" -> JString(rowId),
          "col_num" -> JNumber(24601)
        ))
        val u = for { u <- post("resource", Some(resourceName), Some(rowId), rowBody).either.right } yield u
        u() match {
          case Right(rowResponse) => {
            rowResponse.getStatusCode must equal (200)
          }
          case Left(thr) => throw thr
        }
      }
      case Left(thr) => throw thr
    }
  }
}

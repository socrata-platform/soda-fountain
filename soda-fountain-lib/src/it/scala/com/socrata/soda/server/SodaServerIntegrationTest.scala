package com.socrata.soda.server

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io._
import com.rojoma.json.ast._


class SodaServerIntegrationTest extends IntegrationTest {

  val hostname: String = "localhost:8080"

  def postJson(resourceName: String, body: JValue) = {
    val url = host(hostname) / "resource" / resourceName
    val request = url.POST.
      addHeader("Content-Type", "application/json").
      setBody(body.toString)
    val response = Http(request).either
    response
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

  test("row update request malformed json returns error response"){
    def result = postJson("testDataset", JString("this is not json"))
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => {
        response.getResponseBody.length must be > (0)
        response.getStatusCode must equal (415)
      }
    }
  }

  test("row update request with unexpected format json returns error response"){
    def result = postJson("testDataset", JArray(Array(JString("this is an array"), JString("why would you post an array?"))))
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => response.getStatusCode must equal (400)
    }
  }

  test("soda fountain can create dataset"){
    val body = JObject(Map(
      "resource_name" -> JString("soda-int-create"),
      "name" -> JString("soda integration test create dataset")
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
}

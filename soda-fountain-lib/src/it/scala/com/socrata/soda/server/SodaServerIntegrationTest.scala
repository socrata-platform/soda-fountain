package com.socrata.soda.server

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io._
import com.rojoma.json.ast._


class SodaServerIntegrationTest extends IntegrationTest {

  def localUrl = host("localhost:8080")

  def postJson(body: JValue) = {
    val request = (localUrl / "resource" / "testDataset").
      POST.
      addHeader("Content-Type", "application/json").
      setBody(body.toString)
    val response = Http(request).either
    response
  }

  test("row update request malformed json returns error response"){
    def result = postJson(JString("this is not json"))
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => response.getStatusCode must equal (415)
    }
  }

  test("row update request with unexpected format json returns error response"){
    def result = postJson(JArray(Array(JString("this is an array"), JString("why would you post an array?"))))
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => response.getStatusCode must equal (400)
    }
  }
}

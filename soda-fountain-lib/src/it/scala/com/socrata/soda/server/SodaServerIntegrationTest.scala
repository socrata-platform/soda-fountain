package com.socrata.soda.server

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io._
import com.rojoma.json.ast._


class SodaServerIntegrationTest extends IntegrationTest {

  val host = "localhost:8080"

  def postJson(relativePath: String, body: JValue) = {
    val request = url(host + relativePath).
      POST.
      addHeader("Content-Type", "application/json").
      setBody(body.toString)
    val response = Http(request).either
    response
  }

  test("row update request malformed json returns error response"){
    def result = postJson("/resource/testDataset", JString("this is not json"))
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => response.getStatusCode must equal (415)
    }
  }

  test("row update request with unexpected format json returns error response"){
    def result = postJson("/resource/testDataset", JArray(Array(JString("this is an array"), JString("why would you post an array?"))))
    result() match {
      case Left(exc) => fail(exc.getMessage)
      case Right(response) => response.getStatusCode must equal (400)
    }
  }

//  test("soda fountain can create dataset"){
//
//    def result = postJson("/resources", json)
//  }
}

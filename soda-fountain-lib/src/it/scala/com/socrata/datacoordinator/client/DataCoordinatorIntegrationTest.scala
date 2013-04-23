package com.socrata.datacoordinator.client

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}

class DataCoordinatorIntegrationTest extends FunSuite with MustMatchers {

  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

  def jsonCompare(actual:String, expected:String) = {
    normalizeWhitespace(actual) must equal (normalizeWhitespace(expected))
  }

  def coordinatorCompare(ms: MutationScript, expectedResponse: String){
    val actual = coordinatorGetResponseOrError(ms)
    jsonCompare(actual, expectedResponse)
  }

  def coordinatorGetResponseOrError(ms: MutationScript) = {
    val client = new DataCoordinatorClient("localhost:12345")
    val response = client.sendMutateRequest(ms)
    val actual = response() match {
      case Left(th) => th.getMessage
      case Right(resp) => resp.getResponseBody
    }
    actual
  }
}

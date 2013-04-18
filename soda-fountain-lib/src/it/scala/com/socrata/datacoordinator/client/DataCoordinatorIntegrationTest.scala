package com.socrata.datacoordinator.client

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class DataCoordinatorIntegrationTest extends FunSuite with MustMatchers {

  def coordinatorCompare(ms: MutationScript, expectedResponse: String){
    val client = new DataCoordinatorClient("localhost:12345")
    val response = client.sendMutateRequest(ms)
    val actual = response() match {
      case Left(th) => th.getMessage
      case Right(resp) => resp.getResponseBody
    }
    actual must equal (Right(expectedResponse))
  }
}

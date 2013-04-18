package com.socrata.datacoordinator.client

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._

class DataCoordinatorIntegrationTest extends FunSuite with MustMatchers {

  def coordinatorCompare(ms: MutationScript, expectedResponse: String){
    val client = new DataCoordinatorClient("localhost:12345")
    val response = client.sendMutateRequest(ms)
    val actual = response()
    actual must equal (Right(expectedResponse))
  }
}

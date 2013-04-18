package com.socrata.datacoordinator.client

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

class DataCoordinatorIntegrationTest extends FunSuite with MustMatchers {

  def coordinatorCompare(ms: MutationScript, expectedResponse: String){
    val client = new DataCoordinatorClient("localhost:12345")
    val response = client.mutate(ms)
    response.toString must equal (expectedResponse)

  }


}

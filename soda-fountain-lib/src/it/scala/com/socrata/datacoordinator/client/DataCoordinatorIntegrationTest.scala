package com.socrata.datacoordinator.client

import com.socrata.soda.server.SodaFountainIntegrationTest
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.Success
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient

trait DataCoordinatorIntegrationTest extends SodaFountainIntegrationTest {

  val dc: DataCoordinatorClient = ???
  val userName = "daniel_the_tester"
  val instance = "test_instance"
  val mockSchemaString = "mock_schema"

  def assertSuccess(response: DataCoordinatorClient.Result) = response match {
    case Success(_) => Unit
    case _ => fail(s"response not a success: ${response}")
  }

}

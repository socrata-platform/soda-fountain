package com.socrata.datacoordinator.client

import dispatch._
import com.socrata.soda.server.{SodaFountain, IntegrationTest}
import com.socrata.soda.server.mocks.{MockNameAndSchemaStore, LocalDataCoordinator}
import com.socrata.querycoordinator.client.{LocalQueryCoordinatorClient, QueryCoordinatorClient}

class DataCoordinatorIntegrationTest extends IntegrationTest {

  val fountain = new SodaFountain with MockNameAndSchemaStore with LocalDataCoordinator with LocalQueryCoordinatorClient

  def coordinatorCompare(datasetId: String, ms: MutationScript, expectedResponse: String){
    val actual = coordinatorGetResponseOrError(datasetId, ms)
    jsonCompare(actual, expectedResponse)
  }

  def coordinatorGetResponseOrError(datasetId: String, ms: MutationScript) = {
    val response = fountain.dc.sendMutateRequest(datasetId, ms)
    val actual = response() match {
      case Left(th) => th.getMessage
      case Right(resp) => resp.getResponseBody
    }
    actual
  }
}

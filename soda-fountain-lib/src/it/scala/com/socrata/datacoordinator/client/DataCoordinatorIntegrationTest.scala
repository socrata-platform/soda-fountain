package com.socrata.datacoordinator.client

import dispatch._
import com.socrata.soda.server.{SodaFountain, IntegrationTest}
import com.socrata.soda.server.mocks.{MockNameAndSchemaStore, LocalDataCoordinator}

class DataCoordinatorIntegrationTest extends IntegrationTest {

  val fountain = new SodaFountain with MockNameAndSchemaStore with LocalDataCoordinator

  def coordinatorCompare(datasetId: BigDecimal, ms: MutationScript, expectedResponse: String){
    val actual = coordinatorGetResponseOrError(datasetId, ms)
    jsonCompare(actual, expectedResponse)
  }

  def coordinatorGetResponseOrError(datasetId: BigDecimal, ms: MutationScript) = {
    val response = fountain.dc.sendMutateRequest(datasetId, ms)
    val actual = response() match {
      case Left(th) => th.getMessage
      case Right(resp) => resp.getResponseBody
    }
    actual
  }
}

package com.socrata.datacoordinator.client

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import com.socrata.soda.server.{SodaFountain, IntegrationTest}
import com.socrata.soda.server.mocks.{MockNameAndSchemaStore, LocalDataCoordinator}
import org.scalatest.ParallelTestExecution

class DataCoordinatorIntegrationTest extends IntegrationTest with ParallelTestExecution {

  val fountain = new SodaFountain with MockNameAndSchemaStore with LocalDataCoordinator

  def coordinatorCompare(dataset: String, ms: MutationScript, expectedResponse: String){
    val actual = coordinatorGetResponseOrError(dataset, ms)
    jsonCompare(actual, expectedResponse)
  }

  def coordinatorGetResponseOrError(dataset: String, ms: MutationScript) = {
    val response = fountain.dc.sendMutateRequest(dataset, ms)
    val actual = response() match {
      case Left(th) => th.getMessage
      case Right(resp) => resp.getResponseBody
    }
    actual
  }
}

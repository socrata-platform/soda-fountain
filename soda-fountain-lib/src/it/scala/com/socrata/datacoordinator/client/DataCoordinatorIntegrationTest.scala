package com.socrata.datacoordinator.client

import com.socrata.soda.server.IntegrationTest
import com.socrata.soda.clients.datacoordinator.{CuratedHttpDataCoordinatorClient, DataCoordinatorClient}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.Success
import com.socrata.soda.server.config.SodaFountainConfig
import com.typesafe.config.ConfigFactory
import com.socrata.http.common.AuxiliaryData
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder
import com.socrata.http.client.InetLivenessChecker

trait DataCoordinatorIntegrationTest extends IntegrationTest {

  val dc: DataCoordinatorClient = ???
  val userName = "daniel_the_tester"
  val instance = "test_instance"
  val mockSchemaString = "mock_schema"

  def assertSuccess(response: DataCoordinatorClient.Result) = response match {
    case Success(_) => Unit
    case _ => fail(s"response not a success: ${response}")
  }

}

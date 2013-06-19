package com.socrata.soda.server.persistence

import com.socrata.soda.server.{SodaFountain, IntegrationTest}
import com.socrata.querycoordinator.client.LocalQueryCoordinatorClient
import com.socrata.soda.server.mocks.LocalDataCoordinator
import dispatch._

class PostgresStoreIntegrationTest extends IntegrationTest {

  test("Postgress add/get/remove resourceName and datasetId"){
    val time = System.currentTimeMillis().toString
    val resourceName: String = "postgres name store integration test @" + time
    val datasetId: String = "postgres.name.store.test @" + time
    val a = fountain.store.add(resourceName, datasetId)
    a()
    val f = fountain.store.translateResourceName(resourceName)
    f() match {
      case Right(savedId) => savedId must equal (datasetId)
      case Left(err) => fail(err)
    }
    val d = fountain.store.remove(resourceName)
    d()
    val f2 = fountain.store.translateResourceName(resourceName)
    f2() match {
      case Right(savedId) => fail("resource name should have been removed")
      case Left(err) => {}
    }
  }
}

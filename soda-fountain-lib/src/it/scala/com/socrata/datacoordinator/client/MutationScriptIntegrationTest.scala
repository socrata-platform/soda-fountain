package com.socrata.datacoordinator.client
import java.io._

class MutationScriptIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can hit data coordinator"){
    val mc = new MutationScript("test dataset", "Daniel the tester", UpdateDataset(), Array().toIterable)
    val expected = """["response?"]""".stripMargin
    coordinatorCompare(mc, expected)
  }

  test("Mutation Script can create dataset"){
    val mc = new MutationScript("it_test_1", "Daniel the tester", CreateDataset(), Array().toIterable)
    val expected = """["response to create?"]""".stripMargin
    coordinatorCompare(mc, expected)
  }
}
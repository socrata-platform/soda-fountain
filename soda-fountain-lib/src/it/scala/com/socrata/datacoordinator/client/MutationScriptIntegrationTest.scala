package com.socrata.datacoordinator.client
import java.io._

class MutationScriptTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can hit data coordinator"){
    val mc = new MutationScript("test dataset", "Daniel the tester", UpdateDataset(), Array().toIterable)
    val expected = """[{c:'normal', dataset:'test dataset', user:'Daniel the tester'}]""".stripMargin
    //testCompare(mc, expected)
  }
}
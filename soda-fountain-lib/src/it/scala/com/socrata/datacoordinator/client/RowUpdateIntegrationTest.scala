package com.socrata.datacoordinator.client

import com.socrata.soda.server.persistence.MockStore

class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  test("can declare row data"){
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError("it_row_data_noop", createScript)
    val rowDataNoop = RowUpdateOptionChange(true, false, true)
    val rowDataScript = new MutationScript("Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Right(rowDataNoop)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare("it_row_data_noop", rowDataScript, expected)
  }
}
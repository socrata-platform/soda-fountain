package com.socrata.datacoordinator.client

class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  test("can declare row data"){
    val createScript = new MutationScript("it_row_data_noop", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val rowDataNoop = RowUpdateOptionChange(true, false, true)
    val rowDataScript = new MutationScript("it_row_data_noop", "Daniel the tester", UpdateDataset(), Array(Right(rowDataNoop)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(rowDataScript, expected)
  }
}
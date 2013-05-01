package com.socrata.datacoordinator.client


class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  test("can declare row data"){
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError("it_row_data_noop", createScript)
    val rowDataNoop = RowUpdateOptionChange(true, false, true)
    val rowDataScript = new MutationScript("Daniel the tester", UpdateDataset(fountain.store.getSchemaHash("it_row_data")), Array(Right(rowDataNoop)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare("it_row_data_noop", rowDataScript, expected)
  }
}
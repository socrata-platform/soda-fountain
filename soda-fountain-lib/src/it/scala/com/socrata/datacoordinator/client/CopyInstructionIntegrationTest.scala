package com.socrata.datacoordinator.client


class CopyInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can hit data coordinator"){
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError("it_test_noop", createScript)
    val mc = new MutationScript("Daniel the tester", UpdateDataset(fountain.store.getSchemaHash("mocked")), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare("it_test_noop", mc, expected)
  }

  test("Mutation Script can cause an error"){
    val mc = new MutationScript( "Daniel the tester", UpdateDataset(fountain.store.getSchemaHash("mocked")), Array().toIterable)
    val expected = """{
                     |  "errorCode" : "update.dataset.does-not-exist",
                     |  "data" : { "dataset" : "test dataset" }
                     |}""".stripMargin
    coordinatorCompare("test dataset", mc, expected)
  }

  //if I can't delete the initial copy, then how can I test the create?
  /*
  test("Mutation Script can create dataset"){
    val delScript = new MutationScript("it_test_drop_then_create", "Daniel the tester", DropDataset(), Array().toIterable)
    coordinatorGetResponseOrError(delScript)
    val createScript = new MutationScript("it_test_drop_then_create", "Daniel the tester", CreateDataset(), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(createScript, expected)
  }
  */

  test("Mutation Script can publish dataset"){
    val datasetName = "it_test_publish"
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, createScript)
    val copyScript = new MutationScript("Daniel the tester", CopyDataset(false, fountain.store.getSchemaHash("mocked")), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, copyScript)
    val pubScript = new MutationScript("Daniel the tester", PublishDataset(None, fountain.store.getSchemaHash("mocked")), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, pubScript, expected)
  }

  //need to publish before creating copy
  test("Mutation Script can copy dataset"){
    val datasetName = "it_test_copy"
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, createScript)
    val delScript = new MutationScript("Daniel the tester", DropDataset(fountain.store.getSchemaHash("mocked")), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, delScript)
    val pubScript = new MutationScript("Daniel the tester", PublishDataset(None, fountain.store.getSchemaHash("mocked")), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, pubScript)
    val copyScript = new MutationScript("Daniel the tester", CopyDataset(false, fountain.store.getSchemaHash("mocked")), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, copyScript, expected)
  }

  //need to publish before creating copy
  test("Mutation Script can drop dataset"){
    val datasetName = "it_test_drop"
    val createScript = new MutationScript( "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, createScript)
    val delScript = new MutationScript( "Daniel the tester", DropDataset(fountain.store.getSchemaHash("mocked")), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, delScript)
    val pubScript = new MutationScript( "Daniel the tester", PublishDataset(None, fountain.store.getSchemaHash("mocked")), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, pubScript)
    val copyScript = new MutationScript("Daniel the tester", CopyDataset(false, fountain.store.getSchemaHash("mocked")), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, copyScript)
    val delScript2 = new MutationScript("Daniel the tester", DropDataset(fountain.store.getSchemaHash("mocked")), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, delScript2, expected)
  }


  /*
  test("Mutation Script can set dataset row id"){
    val createScript = new MutationScript("it_test_set_row_id", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val dropIdScript = new MutationScript("it_test_set_row_id", "Daniel the tester", DropRowIdColumnInstruction("row_id_col"), Array().toIterable)
    coordinatorGetResponseOrError(dropIdScript)
    val setIdScript = new MutationScript("it_test_set_row_id", "Daniel the tester", SetRowIdColumnInstruction("row_id_col"), Array().toIterable)
    val expected = """["response to set row id?"]""".stripMargin
    coordinatorCompare(setIdScript, expected)
  }

  test("Mutation Script can drop dataset row id"){
    val createScript = new MutationScript("it_test_drop_row_id", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val setIdScript = new MutationScript("it_test_drop_row_id", "Daniel the tester", SetRowIdColumnInstruction("row_id_col"), Array().toIterable)
    coordinatorGetResponseOrError(setIdScript)
    val dropIdScript = new MutationScript("it_test_drop_row_id", "Daniel the tester", DropRowIdColumnInstruction("row_id_col"), Array().toIterable)
    val expected = """["response to drop row id?"]""".stripMargin
    coordinatorCompare(dropIdScript, expected)
  }
  */
}
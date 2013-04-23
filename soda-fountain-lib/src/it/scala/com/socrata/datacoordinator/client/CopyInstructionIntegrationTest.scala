package com.socrata.datacoordinator.client

class CopyInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can hit data coordinator"){
    val createScript = new MutationScript("it_test_noop", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val mc = new MutationScript("it_test_noop", "Daniel the tester", UpdateDataset(), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(mc, expected)
  }

  test("Mutation Script can cause an error"){
    val mc = new MutationScript("test dataset", "Daniel the tester", UpdateDataset(), Array().toIterable)
    val expected = """{
                     |  "errorCode" : "update.dataset.does-not-exist",
                     |  "data" : { "dataset" : "test dataset" }
                     |}""".stripMargin
    coordinatorCompare(mc, expected)
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
    val createScript = new MutationScript("it_test_publish", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val copyScript = new MutationScript("it_test_publish", "Daniel the tester", CopyDataset(false), Array().toIterable)
    coordinatorGetResponseOrError(copyScript)
    val pubScript = new MutationScript("it_test_publish", "Daniel the tester", PublishDataset(None), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(pubScript, expected)
  }

  //need to publish before creating copy
  test("Mutation Script can copy dataset"){
    val createScript = new MutationScript("it_test_copy", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val delScript = new MutationScript("it_test_copy", "Daniel the tester", DropDataset(), Array().toIterable)
    coordinatorGetResponseOrError(delScript)
    val pubScript = new MutationScript("it_test_copy", "Daniel the tester", PublishDataset(None), Array().toIterable)
    coordinatorGetResponseOrError(pubScript)
    val copyScript = new MutationScript("it_test_copy", "Daniel the tester", CopyDataset(false), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(copyScript, expected)
  }

  //need to publish before creating copy
  test("Mutation Script can drop dataset"){
    val createScript = new MutationScript("it_test_drop", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val delScript = new MutationScript("it_test_drop", "Daniel the tester", DropDataset(), Array().toIterable)
    coordinatorGetResponseOrError(delScript)
    val pubScript = new MutationScript("it_test_drop", "Daniel the tester", PublishDataset(None), Array().toIterable)
    coordinatorGetResponseOrError(pubScript)
    val copyScript = new MutationScript("it_test_drop", "Daniel the tester", CopyDataset(false), Array().toIterable)
    coordinatorGetResponseOrError(copyScript)
    val delScript2 = new MutationScript("it_test_drop", "Daniel the tester", DropDataset(), Array().toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(delScript2, expected)
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
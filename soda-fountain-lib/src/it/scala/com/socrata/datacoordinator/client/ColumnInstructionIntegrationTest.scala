package com.socrata.datacoordinator.client

import com.socrata.soda.server.persistence.MockStore

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can add column"){
    val datasetName = "it_col_add"
    val createScript = new MutationScript( "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName,  createScript)
    val dropColOp = DropColumnInstruction("new_col")
    val dropColScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(dropColOp)).toIterable)
    coordinatorGetResponseOrError(datasetName,  dropColScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val addColScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(addColOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, addColScript, expected)
  }

  test("Mutation Script can drop column"){
    val datasetName = "it_col_drop"
    val createScript = new MutationScript( "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName,  createScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val dropColOp = DropColumnInstruction("new_col")
    val addColScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(datasetName, addColScript)
    val dropColScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(dropColOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, dropColScript, expected)
  }

  test("can set row id"){
    val datasetName = "it_col_set_row_id"
    val createScript = new MutationScript( "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName,  createScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val addColScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(datasetName,  addColScript)
    val dropRowIdOp = DropRowIdColumnInstruction("new_col")
    val dropRowIdScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(dropRowIdOp)).toIterable)
    coordinatorGetResponseOrError(datasetName,  dropRowIdScript)
    val setRowIdOp = SetRowIdColumnInstruction("new_col")
    val setRowIdScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(setRowIdOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, setRowIdScript, expected)
  }

  test("can drop row id"){
    val datasetName = "it_col_drop_row_id"
    val createScript = new MutationScript( "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, createScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val addColScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(datasetName,  addColScript)
    val setRowIdOp = SetRowIdColumnInstruction("new_col")
    val setRowIdScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(setRowIdOp)).toIterable)
    coordinatorGetResponseOrError( datasetName,   setRowIdScript)
    val dropRowIdOp = DropRowIdColumnInstruction("new_col")
    val dropRowIdScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(dropRowIdOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, dropRowIdScript, expected)
  }

  test("can rename column"){
    val datasetName = "it_col_rename"
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(datasetName, createScript)
    val dropColOp1 = DropColumnInstruction("named_col")
    val dropColScript1 = new MutationScript("Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(dropColOp1)).toIterable)
    coordinatorGetResponseOrError(datasetName,  dropColScript1)
    val dropColOp2 = DropColumnInstruction("renamed_col")
    val dropColScript2 = new MutationScript("Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(dropColOp2)).toIterable)
    coordinatorGetResponseOrError(datasetName,  dropColScript2)
    val addColOp = AddColumnInstruction("named_col", Number() )
    val addColScript = new MutationScript("Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(datasetName,  addColScript)
    val renameColumnOp = RenameColumnInstruction("named_col", "renamed_col")
    val renameColScript = new MutationScript("Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Left(renameColumnOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(datasetName, renameColScript, expected)
  }


  test("can declare row data"){
    val createScript = new MutationScript("Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError("it_row_data_noop", createScript)
    val rowDataNoop = RowUpdateOptionChange(true, false, true)
    val rowDataScript = new MutationScript( "Daniel the tester", UpdateDataset(MockStore.blankSchemaHash), Array(Right(rowDataNoop)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare("it_row_data_noop", rowDataScript, expected)
  }
}
package com.socrata.datacoordinator.client

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can add column"){
    val createScript = new MutationScript("it_col_add", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val dropColOp = DropColumnInstruction("new_col")
    val dropColScript = new MutationScript("it_col_add", "Daniel the tester", UpdateDataset(), Array(Left(dropColOp)).toIterable)
    coordinatorGetResponseOrError(dropColScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val addColScript = new MutationScript("it_col_add", "Daniel the tester", UpdateDataset(), Array(Left(addColOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(addColScript, expected)
  }

  test("Mutation Script can drop column"){
    val createScript = new MutationScript("it_col_drop", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val dropColOp = DropColumnInstruction("new_col")
    val addColScript = new MutationScript("it_col_drop", "Daniel the tester", UpdateDataset(), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(addColScript)
    val dropColScript = new MutationScript("it_col_drop", "Daniel the tester", UpdateDataset(), Array(Left(dropColOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(dropColScript, expected)
  }

  test("can set row id"){
    val createScript = new MutationScript("it_col_set_row_id", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val addColScript = new MutationScript("it_col_set_row_id", "Daniel the tester", UpdateDataset(), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(addColScript)
    val dropRowIdOp = DropRowIdColumnInstruction("new_col")
    val dropRowIdScript = new MutationScript("it_col_set_row_id", "Daniel the tester", UpdateDataset(), Array(Left(dropRowIdOp)).toIterable)
    coordinatorGetResponseOrError(dropRowIdScript)
    val setRowIdOp = SetRowIdColumnInstruction("new_col")
    val setRowIdScript = new MutationScript("it_col_set_row_id", "Daniel the tester", UpdateDataset(), Array(Left(setRowIdOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(setRowIdScript, expected)
  }

  test("can drop row id"){
    val createScript = new MutationScript("it_col_drop_row_id", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val addColOp = AddColumnInstruction("new_col", Number() )
    val addColScript = new MutationScript("it_col_drop_row_id", "Daniel the tester", UpdateDataset(), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(addColScript)
    val setRowIdOp = SetRowIdColumnInstruction("new_col")
    val setRowIdScript = new MutationScript("it_col_drop_row_id", "Daniel the tester", UpdateDataset(), Array(Left(setRowIdOp)).toIterable)
    coordinatorGetResponseOrError(setRowIdScript)
    val dropRowIdOp = DropRowIdColumnInstruction("new_col")
    val dropRowIdScript = new MutationScript("it_col_drop_row_id", "Daniel the tester", UpdateDataset(), Array(Left(dropRowIdOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(dropRowIdScript, expected)
  }

  test("can rename column"){
    val createScript = new MutationScript("it_col_rename", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val dropColOp1 = DropColumnInstruction("named_col")
    val dropColScript1 = new MutationScript("it_col_rename", "Daniel the tester", UpdateDataset(), Array(Left(dropColOp1)).toIterable)
    coordinatorGetResponseOrError(dropColScript1)
    val dropColOp2 = DropColumnInstruction("renamed_col")
    val dropColScript2 = new MutationScript("it_col_rename", "Daniel the tester", UpdateDataset(), Array(Left(dropColOp2)).toIterable)
    coordinatorGetResponseOrError(dropColScript2)
    val addColOp = AddColumnInstruction("named_col", Number() )
    val addColScript = new MutationScript("it_col_rename", "Daniel the tester", UpdateDataset(), Array(Left(addColOp)).toIterable)
    coordinatorGetResponseOrError(addColScript)
    val renameColumnOp = RenameColumnInstruction("named_col", "renamed_col")
    val renameColScript = new MutationScript("it_col_rename", "Daniel the tester", UpdateDataset(), Array(Left(renameColumnOp)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(renameColScript, expected)
  }


  test("can declare row data"){
    val createScript = new MutationScript("it_row_data_noop", "Daniel the tester", CreateDataset(), Array().toIterable)
    coordinatorGetResponseOrError(createScript)
    val rowDataNoop = RowUpdateOptionChange(true, false, true)
    val rowDataScript = new MutationScript("it_row_data_noop", "Daniel the tester", UpdateDataset(), Array(Right(rowDataNoop)).toIterable)
    val expected = """[]""".stripMargin
    coordinatorCompare(rowDataScript, expected)
  }
}
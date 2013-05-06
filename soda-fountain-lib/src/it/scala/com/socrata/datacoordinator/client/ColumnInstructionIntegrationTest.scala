package com.socrata.datacoordinator.client

import dispatch._

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can add/drop column"){
    val idAndHash = fountain.dc.create("it_col_add_drop", userName, None).right.getOrElse(throw new Error("could not create dataset"))
    val c = fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(AddColumnInstruction("new_col", Number())).toIterable)
    c() match {
      case Right(r) => r must equal ("""[]""".stripMargin)
      case Left(e) => throw e
    }
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(DropColumnInstruction("new_col", Number())).toIterable)().right.getOrElse(throw New Error("could not create column")) must equal ("""[]""".stripMargin)
  }

  test("can set/drop row id column"){
    val idAndHash = fountain.dc.create("it_col_set_drop_row_id", userName, None).right.getOrElse(throw new Error("could not create dataset"))
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(AddColumnInstruction("new_col", Number())).toIterable)().right.getOrElse(throw New Error("could not create column")) must equal ("""[]""".stripMargin)
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(SetRowIdColumnInstruction("new_col")).toIterable)().right.getOrElse(throw New Error("could not set row id column")) must equal ("""[]""".stripMargin)
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(DropRowIdColumnInstruction("new_col")).toIterable)().right.getOrElse(throw New Error("could not drop row id column")) must equal ("""[]""".stripMargin)
  }

  test("can rename column"){
    val idAndHash = fountain.dc.create("it_col_rename", userName, None).right.getOrElse(throw new Error("could not create dataset"))
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(AddColumnInstruction("named_col", Number())).toIterable)().right.getOrElse(throw New Error("could not create column")) must equal ("""[]""".stripMargin)
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(RenameColumnInstruction("named_col", "renamed_col")).toIterable)().right.getOrElse(throw New Error("could not rename column")) must equal ("""[]""".stripMargin)
  }


  test("can declare row data"){
    val idAndHash = fountain.dc.create("it_declare_row_data", userName, None).right.getOrElse(throw new Error("could not create dataset"))
    fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(RowUpdateOptionChange(true, false, true)).toIterable)().right.getOrElse(throw New Error("could not declare row data")) must equal ("""[]""".stripMargin)
  }
}
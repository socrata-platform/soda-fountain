package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can add/drop column"){
    val responses = for {
      idAndHash <- fountain.dc.create("it_col_add_drop", userName, None).right
      colCreate <- fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(AddColumnInstruction("new_col", Number())).toIterable).right
      colDrop <- fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(DropColumnInstruction("new_col")).toIterable).right
    } yield (idAndHash, colCreate, colDrop)

    responses() match {
      case Right((idAndHash, colCreate, colDrop)) => {
        colCreate.getResponseBody must equal ("""[]""".stripMargin)
        colDrop.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }

  test("can set/drop row id column"){
    val responses = for {
      idAndHash <-fountain.dc.create("it_col_set_drop_row_id", userName, None).right
      newCol <- fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(AddColumnInstruction("new_col", Number())).toIterable).right
      setId <-  fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(SetRowIdColumnInstruction("new_col")).toIterable).right
      dropId <- fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(DropRowIdColumnInstruction("new_col")).toIterable).right
    } yield (idAndHash, newCol, setId, dropId)

    responses() match {
      case Right((idAndHash, newCol, setId, dropId)) => {
        newCol.getResponseBody must equal ("""[]""".stripMargin)
        setId.getResponseBody must equal ("""[]""".stripMargin)
        dropId.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }

  test("can rename column"){
    val responses = for {
      idAndHash <-fountain.dc.create("it_col_rename", userName, None).right
      namedCol <- fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(AddColumnInstruction("named_col", Number())).toIterable).right
      renamedCol <-fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(RenameColumnInstruction("named_col", "renamed_col")).toIterable).right
    } yield (idAndHash, namedCol, renamedCol)

    responses() match {
      case Right((idAndHash, namedCol, renamedCol)) => {
        namedCol.getResponseBody must equal ("""[]""".stripMargin)
        renamedCol.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }
}
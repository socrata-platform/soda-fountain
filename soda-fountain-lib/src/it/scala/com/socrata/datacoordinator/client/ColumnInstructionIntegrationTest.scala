package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can add/drop column"){
    val responses = for {
      idAndResults <- fountain.dc.create("it_col_add_drop", userName, None).right
      colCreate <- fountain.dc.update(idAndResults._1, None, userName, Array( new AddColumnInstruction("new_col", "number")).iterator).right
      colDrop <- fountain.dc.update(idAndResults._1, None, userName, Array(DropColumnInstruction("new_col")).iterator).right
    } yield (idAndResults, colCreate, colDrop)

    responses() match {
      case Right((idAndResults, colCreate, colDrop)) => {
        colCreate.getResponseBody must equal ("""[]""".stripMargin)
        colDrop.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }

  test("can set/drop row id column"){
    val responses = for {
      idAndResults <-fountain.dc.create("it_col_set_drop_row_id", userName, None).right
      newCol <- fountain.dc.update(idAndResults._1, None, userName, Array(new AddColumnInstruction("new_col", "number")).iterator).right
      setId <-  fountain.dc.update(idAndResults._1, None, userName, Array(SetRowIdColumnInstruction("new_col")).iterator).right
      dropId <- fountain.dc.update(idAndResults._1, None, userName, Array(DropRowIdColumnInstruction("new_col")).iterator).right
    } yield (idAndResults, newCol, setId, dropId)

    responses() match {
      case Right((idAndResults, newCol, setId, dropId)) => {
        newCol.getResponseBody must equal ("""[]""".stripMargin)
        setId.getResponseBody must equal ("""[]""".stripMargin)
        dropId.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }

  test("can rename column"){
    val responses = for {
      idAndResults <-fountain.dc.create("it_col_rename", userName, None).right
      namedCol <- fountain.dc.update(idAndResults._1, None, userName, Array(new AddColumnInstruction("named_col", "number")).iterator).right
      renamedCol <-fountain.dc.update(idAndResults._1, None, userName, Array(RenameColumnInstruction("named_col", "renamed_col")).iterator).right
    } yield (idAndResults, namedCol, renamedCol)

    responses() match {
      case Right((idAndResults, namedCol, renamedCol)) => {
        namedCol.getResponseBody must equal ("""[]""".stripMargin)
        renamedCol.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }
}
package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.TypeName
import com.socrata.soda.server.types.ColumnId

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can add/drop column"){
    val id = ColumnId("new_col")
    val typ = SoQLType.typesByName(TypeName("number"))
    val responses = for {
      idAndResults <- fountain.dc.create(userName, None)
      colCreate <- fountain.dc.update(idAndResults._1, None, userName, Array( new AddColumnInstruction(typ, "new_column", Some(id))).iterator)
      colDrop <- fountain.dc.update(idAndResults._1, None, userName, Array(DropColumnInstruction(id)).iterator)
    } yield (idAndResults, colCreate, colDrop)

    responses match {
      case Success((idAndResults, colCreate, colDrop)) => {
        //colCreate.getResponseBody must equal ("""[]""".stripMargin)
        //colDrop.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Failure(thr) => throw thr
    }
  }

  test("can set/drop row id column"){
    val id = ColumnId("id_column")
    val typ = SoQLType.typesByName(TypeName("number"))
    val responses = for {
      idAndResults <-fountain.dc.create(userName, None)
      newCol <- fountain.dc.update(idAndResults._1, None, userName, Array(new AddColumnInstruction(typ, "new_col", Some(id))).iterator)
      setId <-  fountain.dc.update(idAndResults._1, None, userName, Array(SetRowIdColumnInstruction(id)).iterator)
      dropId <- fountain.dc.update(idAndResults._1, None, userName, Array(DropRowIdColumnInstruction(id)).iterator)
    } yield (idAndResults, newCol, setId, dropId)

    responses match {
      case Success((idAndResults, newCol, setId, dropId)) => {
        //newCol.getResponseBody must equal ("""[]""".stripMargin)
        //setId.getResponseBody must equal ("""[]""".stripMargin)
        //dropId.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Failure(thr) => throw thr
    }
  }

  test("can rename column"){ pending } //data coordinator no longer handles column rename. TODO: replace with test for name store column rename
  /*
    val id = ColumnId("col_for_rename")
    val typ = SoQLType.typesByName(TypeName("number"))
    val responses = for {
      idAndResults <-fountain.dc.create(userName, None)
      namedCol <- fountain.dc.update(idAndResults._1, None, userName, Array(new AddColumnInstruction(typ, "named_col", id)).iterator)
      renamedCol <-fountain.dc.update(idAndResults._1, None, userName, Array(new RenameColumnInstruction("named_col", "renamed_col")).iterator)
    } yield (idAndResults, namedCol, renamedCol)

    responses match {
      case Success((idAndResults, namedCol, renamedCol)) => {
        //namedCol.getResponseBody must equal ("""[]""".stripMargin)
        //renamedCol.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Failure(thr) => throw thr
    }
  }
  */
}
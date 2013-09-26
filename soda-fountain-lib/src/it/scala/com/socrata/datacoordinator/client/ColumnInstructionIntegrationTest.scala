package com.socrata.datacoordinator.client

import scala.util.{Failure, Success}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.TypeName
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.clients.datacoordinator._

class ColumnInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can add/drop column"){
    val id = ColumnId("new_col")
    val typ = SoQLType.typesByName(TypeName("number"))
    val idAndResults = dc.create(instance, userName, None)
    dc.update(idAndResults._1, mockSchemaString, userName, Array( new AddColumnInstruction(typ, "new_column", Some(id))).iterator){ colCreateResult =>
      dc.update(idAndResults._1, mockSchemaString, userName, Array(DropColumnInstruction(id)).iterator){ colDropResult =>
        //colCreate.getResponseBody must equal ("""[]""".stripMargin)
        //colDrop.getResponseBody must equal ("""[]""".stripMargin)
      }
    }
  }

  test("can set/drop row id column"){
    val id = ColumnId("id_column")
    val typ = SoQLType.typesByName(TypeName("number"))
    val idAndResults = dc.create(instance, userName, None)
    dc.update(idAndResults._1, mockSchemaString, userName, Array(new AddColumnInstruction(typ, "new_col", Some(id))).iterator){newCol =>
      dc.update(idAndResults._1, mockSchemaString, userName, Array(SetRowIdColumnInstruction(id)).iterator){ setId =>
        dc.update(idAndResults._1, mockSchemaString, userName, Array(DropRowIdColumnInstruction(id)).iterator){dropId =>
          //newCol.getResponseBody must equal ("""[]""".stripMargin)
          //setId.getResponseBody must equal ("""[]""".stripMargin)
          //dropId.getResponseBody must equal ("""[]""".stripMargin)
        }
      }
    }
  }

  test("can rename column"){ pending } //data coordinator no longer handles column rename. TODO: replace with test for name store column rename
  /*
    val id = ColumnId("col_for_rename")
    val typ = SoQLType.typesByName(TypeName("number"))
    val responses = for {
      idAndResults <-dc.create(instance, userName, None)
      namedCol <- dc.update(idAndResults._1, mockSchemaString, userName, Array(new AddColumnInstruction(typ, "named_col", id)).iterator)
      renamedCol <-dc.update(idAndResults._1, mockSchemaString, userName, Array(new RenameColumnInstruction("named_col", "renamed_col")).iterator)
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
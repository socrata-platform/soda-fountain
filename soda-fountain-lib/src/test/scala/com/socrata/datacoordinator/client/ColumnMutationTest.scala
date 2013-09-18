package com.socrata.datacoordinator.client
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment._
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.clients.datacoordinator._

class ColumnMutationTest extends DataCoordinatorClientTest {

  val numberType = SoQLType.typesByName(TypeName("number"))
  val id = ColumnId("a column id")
  val hint = "a hint"
  test("Add Column toString produces JSON") {
    val ac = new AddColumnInstruction(numberType, hint, Some(id))
    ac.toString must equal (normalizeWhitespace("{c:'add column', hint:'a hint', type:'number', id:'a column id'}"))
  }
  test("Drop Column toString produces JSON") {
    val ac = new DropColumnInstruction(id)
    ac.toString must equal (normalizeWhitespace("{c:'drop column', column:'a column id'}"))
  }
  test("Set Row ID toString produces JSON") {
    val ac = new SetRowIdColumnInstruction(id)
    ac.toString must equal (normalizeWhitespace("{c:'set row id', column:'a column id'}"))
  }
  test("Drop Row ID toString produces JSON") {
    val ac = new DropRowIdColumnInstruction(id)
    ac.toString must equal (normalizeWhitespace("{c:'drop row id', column:'a column id'}"))
  }
}

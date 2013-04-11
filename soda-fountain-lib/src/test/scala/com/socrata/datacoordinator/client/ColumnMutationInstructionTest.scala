package com.socrata.datacoordinator.client
import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite

class ColumnMutationInstructionTest extends DataCoordinatorClientTest {

  test("Add Column toString produces JSON") {
    val ac = new AddColumnInstruction("new_col", "number")
    ac.toString must equal (normalizeWhitespace("{c:'add column',name:'new_col',type:'number'}"))
  }
  test("Drop Column toString produces JSON") {
    val ac = new DropColumnInstruction("drop_col")
    ac.toString must equal (normalizeWhitespace("{c:'drop column',name:'drop_col'}"))
  }
  test("Rename Column toString produces JSON") {
    val ac = new RenameColumnInstruction("old_name", "new_name")
    ac.toString must equal (normalizeWhitespace("{c:'rename column',from:'old_name',to:'new_name'}"))
  }
  test("Set Row ID toString produces JSON") {
    val ac = new SetRowIdColumnInstruction("id_col")
    ac.toString must equal (normalizeWhitespace("{c:'set row id',name:'id_col'}"))
  }
  test("Drop Row ID toString produces JSON") {
    val ac = new DropRowIdColumnInstruction("drop_id_col")
    ac.toString must equal (normalizeWhitespace("{c:'drop row id',name:'drop_id_col'}"))
  }
}

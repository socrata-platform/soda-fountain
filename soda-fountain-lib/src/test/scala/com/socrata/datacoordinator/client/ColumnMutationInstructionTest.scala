package com.socrata.datacoordinator.client
import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite
import com.rojoma.json.util.JsonUtil


class ColumnMutationInstructionTest extends FunSuite with MustMatchers {

  def norm(fixture: String) = JsonUtil.renderJson((JsonUtil.parseJson[Map[String,String]](fixture).get))

  test("Add Column toString produces JSON") {
    val ac = new AddColumnInstruction("new_col", "number")
    ac.toString must equal (norm("{c:'add column',name:'new_col',type:'number'}"))
  }
  test("Drop Column toString produces JSON") {
    val ac = new DropColumnInstruction("drop_col")
    ac.toString must equal (norm("{c:'drop column',name:'drop_col'}"))
  }
  test("Rename Column toString produces JSON") {
    val ac = new RenameColumnInstruction("old_name", "new_name")
    ac.toString must equal (norm("{c:'rename column',from:'old_name',to:'new_name'}"))
  }
  test("Set Row ID toString produces JSON") {
    val ac = new SetRowIdColumnInstruction("id_col")
    ac.toString must equal (norm("{c:'set row id',name:'id_col'}"))
  }
  test("Drop Row ID toString produces JSON") {
    val ac = new DropRowIdColumnInstruction("drop_id_col")
    ac.toString must equal (norm("{c:'drop row id',name:'drop_id_col'}"))
  }
}

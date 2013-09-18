package com.socrata.datacoordinator.client
import java.io._
import com.rojoma.json.ast.JString
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.TypeName
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.clients.datacoordinator._

class MutationScriptTest extends DataCoordinatorClientTest {

  val schemaString = "change me to the schema"
  val numberType = SoQLType.typesByName(TypeName("number"))
  val columnId = Some(ColumnId("a column id"))
  val hint = "a hint"

  def testCompare(mc: MutationScript, expected: String) {
    val sw = new StringWriter()
    mc.streamJson(sw)
    sw.toString must equal (normalizeWhitespace(expected))
  }

  test("Mutation Script compiles and runs"){
    val mc = new MutationScript("Daniel the tester", UpdateDataset(schemaString), Array().iterator)
    val expected = """[{c:'normal',  user:'Daniel the tester'}]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation"){
    val cm = new AddColumnInstruction(numberType, hint, columnId)
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'add column', hint:'a hint', type:'number', id:'a column id'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row update"){
    val ru = new UpsertRow(Map("a" -> JString("aaa"), "b" -> JString("bbb")))
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row delete"){
    val ru = new DeleteRow(new JString("row id string"))
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | ['row id string']
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a both column mutation and row update"){
    val cm = new AddColumnInstruction(numberType, hint, columnId)
    val ru = new UpsertRow(Map("a" -> JString("aaa"), "b" -> JString("bbb")))
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(cm, ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'add column', hint:'a hint', type:'number', id:'a column id'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation after a row update"){
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val cm = new AddColumnInstruction(numberType, hint, columnId)
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(ru, cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa'},
        | null,
        | {c:'add column', hint:'a hint', type:'number', id:'a column id'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row option change and row update"){
    val roc = new RowUpdateOptionChange(true, false, false)
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(roc, ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'row data',"truncate":true,"update":"replace","fatal_row_errors":false},
        | {a:'aaa'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a multiple row option changes and row update"){
    val roc1 = new RowUpdateOptionChange(true, false, false)
    val roc2 = new RowUpdateOptionChange()
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val mc = new MutationScript(
      "Daniel the tester",
      UpdateDataset(schemaString),
      Array(roc1,roc2,ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester'},
        | {c:'row data',"truncate":true,"update":"replace","fatal_row_errors":false},
        | null,
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa'}
        |]""".stripMargin
    testCompare(mc, expected)
  }
}

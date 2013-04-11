package com.socrata.datacoordinator.client
import java.io._

class MutationScriptTest extends DataCoordinatorClientTest {

  def testCompare(mc: MutationScript, expected: String) {
    val sw = new StringWriter()
    mc.streamJson(sw)
    sw.toString must equal (normalizeWhitespace(expected))
  }

  test("Mutation Script compiles and runs"){
    val mc = new MutationScript("test dataset", "Daniel the tester", UpdateDataset(), Array().toIterable)
    val expected = """[{c:'normal', dataset:'test dataset', user:'Daniel the tester'}]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation"){
    val cm = AddColumnInstruction("column_to_add", "number")
    val mc = new MutationScript(
      "test dataset",
      "Daniel the tester",
      UpdateDataset(),
      Array(Left(cm)).toIterable)
    val expected =
      """[
        | {c:'normal', dataset:'test dataset', user:'Daniel the tester'},
        | {c:'add column', name:'column_to_add', type:'number'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row update"){
    val ru = new UpsertRowInstruction(Map("a" -> "aaa", "b" -> "bbb"))
    val mc = new MutationScript(
      "test dataset",
      "Daniel the tester",
      UpdateDataset(),
      Array(Right(ru)).toIterable)
    val expected =
      """[
        | {c:'normal', dataset:'test dataset', user:'Daniel the tester'},
        | {c:'row data'},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row delete"){
    val ru = new DeleteRowInstruction(Left("aaa"))
    val mc = new MutationScript(
      "test dataset",
      "Daniel the tester",
      UpdateDataset(),
      Array(Right(ru)).toIterable)
    val expected =
      """[
        | {c:'normal', dataset:'test dataset', user:'Daniel the tester'},
        | {c:'row data'},
        | ['aaa']
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a both column mutation and row update"){
    val cm = AddColumnInstruction("b", "text")
    val ru = new UpsertRowInstruction(Map("a" -> "aaa", "b" -> "bbb"))
    val mc = new MutationScript(
      "test dataset",
      "Daniel the tester",
      UpdateDataset(),
      Array(Left(cm), Right(ru)).toIterable)
    val expected =
      """[
        | {c:'normal', dataset:'test dataset', user:'Daniel the tester'},
        | {c:'add column', name:'b', type:'text'},
        | {c:'row data'},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation after a row update"){
    val ru = new UpsertRowInstruction(Map("a" -> "aaa"))
    val cm = AddColumnInstruction("b", "text")
    val mc = new MutationScript(
      "test dataset",
      "Daniel the tester",
      UpdateDataset(),
      Array(Right(ru), Left(cm)).toIterable)
    val expected =
      """[
        | {c:'normal', dataset:'test dataset', user:'Daniel the tester'},
        | {c:'row data'},
        | {a:'aaa'},
        | null,
        | {c:'add column', name:'b', type:'text'}
        |]""".stripMargin
    testCompare(mc, expected)
  }
}

package com.socrata.datacoordinator.client
import java.io._

class MutationScriptTest extends DataCoordinatorClientTest {

  def testCompare(mc: MutationScript, expected: String) = {
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
}

package com.socrata.datacoordinator.client
import java.io._
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.io.JsonReader
import com.socrata.soda.server.wiremodels.{SourceColumnSpec, ComputationStrategyType, ComputationStrategySpec}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soda.server.id.{RollupName, ColumnId}
import com.socrata.soda.clients.datacoordinator._

class MutationScriptTest extends DataCoordinatorClientTest {

  val schemaString = "fake_schema_hash"
  val numberType = SoQLType.typesByName(TypeName("number"))
  val columnId = Some(ColumnId("a column id"))
  val fieldName= ColumnName("field_name")
  val user = "Daniel the tester"
  val computationStrategy = ComputationStrategySpec(
    ComputationStrategyType.Test,
    Some(Seq(SourceColumnSpec(ColumnId("source column id"), ColumnName("source_column")))),
    Some(JObject(Map("param" -> JString("some value"))))
  )

  def testCompare(mc: MutationScript, expected: String) {
    val sw = new StringWriter()
    mc.streamJson(sw)
    JsonReader.fromString(sw.toString) must equal (JsonReader.fromString(expected))
  }

  test("Mutation Script compiles and runs"){
    val mc = new MutationScript(user, UpdateDataset(schemaString), Array().iterator)
    val expected = """[{c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'}]"""
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation"){
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, None)
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation with computation strategy"){
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, Some(computationStrategy))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id', computation_strategy:{
        |  strategy_type:'test',
        |  source_column_ids:['source column id'],
        |  parameters:{param:'some value'}
        | }}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row update"){
    val ru = new UpsertRow(Map("a" -> JString("aaa"), "b" -> JString("bbb")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row delete"){
    val ru = new DeleteRow(new JString("row id string"))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | ['row id string']
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a rollup delete"){
    val dropRollup = new DropRollupInstruction(new RollupName("clown_type"))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(dropRollup).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'drop rollup',"name":"clown_type"}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a both column mutation and row update"){
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, None)
    val ru = new UpsertRow(Map("a" -> JString("aaa"), "b" -> JString("bbb")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(cm, ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation after a row update"){
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, None)
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(ru, cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa'},
        | null,
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row option change and row update"){
    val roc = new RowUpdateOptionChange(true, false, false)
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(roc, ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'row data',"truncate":true,"update":"replace","fatal_row_errors":false},
        | {a:'aaa'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  // ake_schema_hash"},
  // {"[id":"a column id","c":"add column","type":"number","computation_strategy":
  //
  // {"strategy_type":"test"
  // ,"source_column_ids":["source column id"]
  // ,"parameters":{"param":"some value"}}
  // ,"field_name":"field_name"
  //
  // ]}]
  //
  // " did not equal "...
  // ake_schema_hash"},
  // {"[c":"add column","field_name":"field_name","type":"number","id":"a column id","computation_strategy":
  //
  //
  // {"strategy_type":"test"
  // ,"source_column_ids":["source column id"]
  // ,"parameters":{"param":"some value"}}
  //
  //
  // ]}]"

  test("Mutation Script encodes a multiple row option changes and row update"){
    val roc1 = new RowUpdateOptionChange(true, false, false)
    val roc2 = new RowUpdateOptionChange()
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString),
      Array(roc1,roc2,ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash'},
        | {c:'row data',"truncate":true,"update":"replace","fatal_row_errors":false},
        | null,
        | {c:'row data',"truncate":false,"update":"merge","fatal_row_errors":true},
        | {a:'aaa'}
        |]""".stripMargin
    testCompare(mc, expected)
  }
}

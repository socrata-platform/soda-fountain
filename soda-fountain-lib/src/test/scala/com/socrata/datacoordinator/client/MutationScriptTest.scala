package com.socrata.datacoordinator.client
import java.io._
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.io.JsonReader
import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.wiremodels.{SourceColumnSpec, ComputationStrategySpec}
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
    StrategyType.Test,
    Some(Seq(SourceColumnSpec(ColumnId("source column id"), ColumnName("source_column")))),
    Some(JObject(Map("param" -> JString("some value"))))
  )

  def testCompare(mc: MutationScript, expected: String) {
    val sw = new StringWriter()
    mc.streamJson(sw)
    JsonReader.fromString(sw.toString) must equal (JsonReader.fromString(expected))
  }

  test("Mutation Script compiles and runs"){
    val mc = new MutationScript(user, UpdateDataset(schemaString, Some(5)), Array().iterator)
    val expected = """[{c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 5}]"""
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation"){
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, None)
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(11001001)),
      Array(cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 11001001},
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation with computation strategy"){
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, Some(computationStrategy))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(6)),
      Array(cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 6},
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
      UpdateDataset(schemaString, Some(7)),
      Array(ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 7},
        | {c:'row data',"truncate":false,"update":"merge","nonfatal_row_errors":[]},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row delete"){
    val ru = new DeleteRow(new JString("row id string"))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(8)),
      Array(ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 8},
        | {c:'row data',"truncate":false,"update":"merge","nonfatal_row_errors":[]},
        | ['row id string']
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a rollup delete"){
    val dropRollup = new DropRollupInstruction(new RollupName("clown_type"))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(9)),
      Array(dropRollup).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 9},
        | {c:'drop rollup',"name":"clown_type"}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a both column mutation and row update"){
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, None)
    val ru = new UpsertRow(Map("a" -> JString("aaa"), "b" -> JString("bbb")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(10)),
      Array(cm, ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 10},
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id'},
        | {c:'row data',"truncate":false,"update":"merge","nonfatal_row_errors":[]},
        | {a:'aaa', b:'bbb'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a column mutation after a row update"){
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val cm = new AddColumnInstruction(numberType, fieldName, columnId, None)
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(11)),
      Array(ru, cm).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 11},
        | {c:'row data',"truncate":false,"update":"merge","nonfatal_row_errors":[]},
        | {a:'aaa'},
        | null,
        | {c:'add column', field_name:'field_name', type:'number', id:'a column id'}
        |]""".stripMargin
    testCompare(mc, expected)
  }

  test("Mutation Script encodes a row option change and row update"){
    val roc = RowUpdateOption.default.copy(truncate = true, mergeInsteadOfReplace = false, errorPolicy = RowUpdateOption.NoRowErrorsAreFatal)
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(12)),
      Array(roc, ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 12},
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
    val roc1 = RowUpdateOption.default.copy(
            truncate = true,
            mergeInsteadOfReplace = false,
            errorPolicy = RowUpdateOption.NonFatalRowErrors(Array("no_such_row_to_delete"))
    )
    val roc2 = RowUpdateOption.default
    val ru = new UpsertRow(Map("a" -> JString("aaa")))
    val mc = new MutationScript(
      user,
      UpdateDataset(schemaString, Some(13)),
      Array(roc1,roc2,ru).iterator)
    val expected =
      """[
        | {c:'normal',  user:'Daniel the tester', schema:'fake_schema_hash', data_version: 13},
        | {c:'row data',"truncate":true,"update":"replace","nonfatal_row_errors":["no_such_row_to_delete"]},
        | null,
        | {c:'row data',"truncate":false,"update":"merge","nonfatal_row_errors":[]},
        | {a:'aaa'}
        |]""".stripMargin
    testCompare(mc, expected)
  }
}

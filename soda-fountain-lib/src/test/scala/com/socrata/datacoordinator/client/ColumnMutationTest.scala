package com.socrata.datacoordinator.client

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.interpolation._
import com.socrata.computation_strategies._
import com.socrata.soda.server.wiremodels.{SourceColumnSpec, ComputationStrategySpec}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment._
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.clients.datacoordinator._

class ColumnMutationTest extends DataCoordinatorClientTest {

  val numberType = SoQLType.typesByName(TypeName("number"))
  val textType = SoQLType.typesByName(TypeName("text"))
  val pointType = SoQLType.typesByName(TypeName("point"))
  val id = ColumnId("a column id")
  val fieldName = ColumnName("a_field_name")
  val testStrategy = ComputationStrategySpec(
    StrategyType.Test,
    Some(Seq(SourceColumnSpec(ColumnId("source column id"), ColumnName("source_field_name")))),
    Some(JsonEncode.toJValue(TestParameterSchema("foo")).asInstanceOf[JObject])
  )
  val geocodingStrategy = ComputationStrategySpec(
    StrategyType.Geocoding,
    Some(Seq(
      SourceColumnSpec(ColumnId("2222-2222"), ColumnName("street_address")),
        SourceColumnSpec(ColumnId("3333-3333"), ColumnName("zip_code"))
    )),
    Some(JsonEncode.toJValue(GeocodingParameterSchema(
      sources = GeocodingSources(
        address = Some("2222-2222"),
        locality = None,
        region = None,
        subregion = None,
        postalCode = Some("3333-3333"),
        country = None
      ),
      defaults = GeocodingDefaults(
        address = None,
        locality = Some("Seattle"),
        region = Some("WA"),
        subregion = None,
        postalCode = None,
        country = "US"
      ),
      version = "v1"
    )).asInstanceOf[JObject])
  )
  test("Add Column toString produces JSON") {
    val ac = new AddColumnInstruction(numberType, fieldName, Some(id), None)
    JsonReader.fromString(ac.toString) must equal (json"{c:'add column', field_name:'a_field_name', type:'number', id:'a column id'}")
  }
  test("Add Column with test computation strategy toString produces JSON") {
    val ac = new AddColumnInstruction(textType, fieldName, Some(id), Some(testStrategy))
    JsonReader.fromString(ac.toString) must equal(
      json"""{c:'add column'
             ,field_name:'a_field_name'
             ,type:'text'
             ,id:'a column id'
             ,computation_strategy:{strategy_type:'test',source_column_ids:['source column id'],parameters:{concat_text:'foo'}}
             }"""
    )
  }
  test("Add Column with geocoding computation strategy toString produces JSON") {
    val ac = new AddColumnInstruction(pointType, fieldName, Some(id), Some(geocodingStrategy))
    JsonReader.fromString(ac.toString) must equal(
      json"""{c:'add column'
             ,field_name:'a_field_name'
             ,type:'point'
             ,id:'a column id'
             ,computation_strategy:
               {strategy_type:'geocoding'
               ,source_column_ids:['2222-2222','3333-3333']
               ,parameters:
                 {sources:{address:'2222-2222',postal_code:'3333-3333'}
                 ,defaults:{locality:'Seattle',region:'WA',country:'US'}
                 ,version:'v1'
                 }
               }
             }"""
    )
  }
  test("Drop Column toString produces JSON") {
    val ac = new DropColumnInstruction(id)
    JsonReader.fromString(ac.toString) must equal (json"{c:'drop column', column:'a column id'}")
  }
  test("Set Row ID toString produces JSON") {
    val ac = new SetRowIdColumnInstruction(id)
    JsonReader.fromString(ac.toString) must equal (json"{c:'set row id', column:'a column id'}")
  }
  test("Drop Row ID toString produces JSON") {
    val ac = new DropRowIdColumnInstruction(id)
    JsonReader.fromString(ac.toString) must equal (json"{c:'drop row id', column:'a column id'}")
  }
}

package com.socrata.soda.server.services

import org.scalatest.{Matchers, FunSuite}
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast.{JString, JObject}
import com.socrata.soda.server.wiremodels._

class DatasetExtractorTest extends FunSuite with Matchers {
  def extract(input: String) = UserProvidedDatasetSpec.fromObject(JsonUtil.parseJson[JObject](input).get)

  //TODO: enable these tests with a mock http request
  /*
  test("Dataset Extractor blank input"){
    val spec = extract("""""")
    spec.isRight must be (false)
  }
  test("Dataset Extractor unexpected JSON"){
    val spec = extract("""[]""")
    spec.isRight must be (false)
  }
  */

  test("Dataset Extractor works in the simple case"){
    val spec = extract("""{
                         |  resource_name: "hotdog",
                         |  name: "hot dog"
                         |}""".stripMargin)
    spec match {
      case Extracted(dspec) =>
        dspec.resourceName should be eq ("hotdog")
        dspec.name should be eq ("hot dog")
        dspec.columns should be (None)
      case _ => fail("didn't extract")
    }
  }
  test("Dataset Extractor works with a column specified"){
    val spec = extract("""{
                         |  resource_name: "hotdog",
                         |  name: "hot dog",
                         |  columns: [
                         |    {name:"column1",
                         |    field_name:"col1",
                         |    description:"this is a column",
                         |    datatype:"money"}
                         |  ]
                         |}""".stripMargin)
    spec match {
      case Extracted(dspec) =>
        dspec.columns.isDefined should be (true)
        val columns = dspec.columns.get
        columns.size should be (1)
        columns(0).name should be eq (Some("column1"))
        columns(0).fieldName should be eq (Some("col1"))
        columns(0).description should be eq (Some("this is a column"))
        columns(0).datatype should be (Some(com.socrata.soql.types.SoQLMoney))
      case _ => fail("didn't extract")
    }
  }
  test("Dataset Extractor works with a computed column"){
    val spec = extract("""{
                         |  resource_name: "chicago_crimes",
                         |  name: "Chicago Crimes",
                         |  columns: [
                         |    {name: "location",
                         |    field_name: "Location",
                         |    description: "Location of the crime",
                         |    datatype: "point"},
                         |    {name: "ward_id",
                         |    field_name: "Ward ID",
                         |    description: "Ward ID",
                         |    datatype: "number",
                         |    computation_strategy: {
                         |      type: "georegion",
                         |      recompute: true,
                         |      source_columns: ["location"],
                         |      parameters: { georegion_uid:"abcd-1234" }
                         |    }}
                         |  ]
                         |}""".stripMargin)
    spec match {
      case Extracted(dspec) =>
        dspec.columns.isDefined should be (true)
        dspec.columns.get.size should be (2)

        val regionColumn = dspec.columns.get(1)
        regionColumn.name should be eq (Some("ward_id"))
        regionColumn.fieldName should be eq (Some("Ward ID"))
        regionColumn.description should be eq (Some("Ward ID"))
        regionColumn.datatype should be (Some(com.socrata.soql.types.SoQLNumber))
        regionColumn.computationStrategy should not be (None)

        val compStrategy = regionColumn.computationStrategy.get
        compStrategy.strategyType should be eq (Some(ComputationStrategyType.GeoRegion))
        compStrategy.recompute should equal (Some(true))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))

      case _ => fail("didn't extract")
    }
  }
  test("Dataset Extractor fails for a description that is not a string"){
    val spec = extract("""{
                         |  resource_name: "hotdog",
                         |  name: "hot dog",
                         |  description: 2,
                         |  columns: []
                         |}""".stripMargin)
    spec match {
      case Extracted(dspec) =>
        fail("extraction should have failed for the description being a number")
      case _ => {}
    }
  }
  test("Dataset Extractor can collect multiple error messages for failed validations"){
    val spec = extract("""{
                         |  resource_name: "hotdog",
                         |  name: "hot dog",
                         |  description: 2
                         |}""".stripMargin)
    spec match {
      case Extracted(dspec) =>
        fail("extraction should have failed")
      case err: IOProblem => {}
      case err: RequestProblem => {}
      case _ =>
    }
  }

}
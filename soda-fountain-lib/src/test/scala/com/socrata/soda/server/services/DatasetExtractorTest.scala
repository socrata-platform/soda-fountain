package com.socrata.soda.server.services

import org.scalatest.{Matchers, FunSuite}
import com.rojoma.json.v3.io.{JsonReader, CompactJsonWriter}
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast.{JString, JObject}
import com.socrata.soda.server.wiremodels._

class DatasetExtractorTest extends FunSuite with Matchers {
  def extract(input: String) = UserProvidedDatasetSpec.fromObject(JsonUtil.parseJson[JObject](input).right.get)

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
                         |    {field_name:"col1",
                         |    datatype:"money"}
                         |  ]
                         |}""".stripMargin)
    spec match {
      case Extracted(dspec) =>
        dspec.columns.isDefined should be (true)
        val columns = dspec.columns.get
        columns.size should be (1)
        columns(0).fieldName should be eq (Some("col1"))
        columns(0).datatype should be (Some(com.socrata.soql.types.SoQLMoney))
      case _ => fail("didn't extract")
    }
  }
  test("Dataset Extractor works with a computed column"){
    val spec = extract("""{
                         |  resource_name: "chicago_crimes",
                         |  name: "Chicago Crimes",
                         |  columns: [
                         |    {field_name: "Location",
                         |    datatype: "point"},
                         |    {field_name: "Ward ID",
                         |    datatype: "number",
                         |    computation_strategy: {
                         |      type: "georegion_match_on_point",
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
        regionColumn.fieldName should be eq (Some("Ward ID"))
        regionColumn.datatype should be (Some(com.socrata.soql.types.SoQLNumber))
        regionColumn.computationStrategy should not be (None)

        val compStrategy = regionColumn.computationStrategy.get
        compStrategy.strategyType should be eq (Some(ComputationStrategyType.GeoRegionMatchOnPoint))
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
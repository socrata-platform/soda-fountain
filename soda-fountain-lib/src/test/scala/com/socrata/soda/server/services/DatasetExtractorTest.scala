package com.socrata.soda.server.services

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import java.io.StringReader
import com.socrata.soda.server.services.ClientRequestExtractor.DatasetSpec
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast.JObject

class DatasetExtractorTest extends FunSuite with MustMatchers {
  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

  def extract(input: String) = DatasetSpec(JsonUtil.parseJson[JObject](input).get)

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
                         |  name: "hot dog",
                         |  columns: []
                         |}""".stripMargin)
    spec.isRight must be (true)
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
    spec.isRight must be (true)
  }
  test("Dataset Extractor fails for a description that is not a string"){
    val spec = extract("""{
                         |  resource_name: "hotdog",
                         |  name: "hot dog",
                         |  description: 2,
                         |  columns: []
                         |}""".stripMargin)
    spec.isRight must be (false)
  }
  test("Dataset Extractor can collect multiple error messages for failed validations"){
    val spec = extract("""{
                         |  resource_name: "hotdog",
                         |  name: "hot dog",
                         |  description: 2
                         |}""".stripMargin)
    spec match {
      case Right(r) => fail("the dataset json is not valid")
      case Left(ers) => {
        println(ers)
        ers.length must be (2)
      }
    }
  }

}
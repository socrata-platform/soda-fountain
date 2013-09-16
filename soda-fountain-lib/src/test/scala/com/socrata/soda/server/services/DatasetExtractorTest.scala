package com.socrata.soda.server.services

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import java.io.StringReader
import com.socrata.soda.server.services.ClientRequestExtractor._
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast.JObject
import com.socrata.soda.server.services.ClientRequestExtractor.IOProblem
import com.socrata.soda.server.services.ClientRequestExtractor.Extracted

class DatasetExtractorTest extends FunSuite with MustMatchers {
  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

  def extract(input: String) = UserProvidedDatasetSpec(JsonUtil.parseJson[JObject](input).get)

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
    spec match {
      case Extracted(dspec) => {}
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
        dspec.columns.size must be (1)
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
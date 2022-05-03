package com.socrata.soda.server.services

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soda.server.id.IndexName
import com.socrata.soda.server.wiremodels.{Extracted, UserProvidedIndexSpec}
import org.scalatest.{FunSuite, Matchers}

class IndexExtractorTest extends FunSuite with Matchers {
  def extract(input: String) = UserProvidedIndexSpec.fromObject(JsonUtil.parseJson[JObject](input).right.get)

  test("All fields populated") {
    val spec = extract( """{
                        name: "composite_index1",
                        expressions: "col1,col2",
                        filter: "deleted_at is null"
                        }""")
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.name should be (Some(new IndexName("composite_index1" )))
        compStrategy.expressions should be (Some("col1,col2"))
        compStrategy.filter should be (Some("deleted_at is null"))
      case _ => fail("didn't extract")
    }
  }

  test("no filter") {
    val spec = extract( """{
                        name: "composite_index1",
                        expressions: "col1,col2"
                        }""")

    spec match {
      case Extracted(compStrategy) =>
        compStrategy.name should be (Some(new IndexName("composite_index1" )))
        compStrategy.expressions should be (Some("col1,col2"))
        compStrategy.filter should be (None)
      case _ => fail(
        "didn't extract")
    }
  }
}

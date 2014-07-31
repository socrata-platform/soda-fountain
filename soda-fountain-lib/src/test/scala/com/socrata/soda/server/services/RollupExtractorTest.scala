package com.socrata.soda.server.services

import com.rojoma.json.ast.JObject
import com.rojoma.json.util.JsonUtil
import com.socrata.soda.server.id.RollupName
import com.socrata.soda.server.wiremodels.{Extracted, UserProvidedRollupSpec}
import org.scalatest.{FunSuite, Matchers}

class RollupExtractorTest extends FunSuite with Matchers {
  def extract(input: String) = UserProvidedRollupSpec.fromObject(JsonUtil.parseJson[JObject](input).get)

  test("All fields populated") {
    val spec = extract( """{
                         |  name: "clown_type",
                         |  soql: "select clown_type, count(*)"}
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.name should be (Some(new RollupName("clown_type" )))
        compStrategy.soql should be (Some("select clown_type, count(*)"))
      case _ => fail("didn't extract")
    }
  }

  test("no soql") {
    val spec = extract( """{
                         |  name: "clown_type"
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.name should be (Some(new RollupName("clown_type" )))
        compStrategy.soql should be (None)
      case _ => fail(
          "didn't extract")
    }
  }
}
package com.socrata.soda.server.services

import org.scalatest.{Matchers, FunSuite}
import com.socrata.soda.server.wiremodels.{RequestProblem, ComputationStrategyType, Extracted, UserProvidedComputationStrategySpec}
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast.{JString, JObject}
import com.socrata.soda.server.errors.ComputationStrategySpecUnknownType

class ComputationStrategyExtractorTest extends FunSuite with Matchers {
  def extract(input: String) = UserProvidedComputationStrategySpec.fromObject(JsonUtil.parseJson[JObject](input).get)

  test("All fields populated") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  recompute: true,
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(ComputationStrategyType.GeoRegionMatchOnPoint))
        compStrategy.recompute should equal (Some(true))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))

      case _ => fail("didn't extract")
    }
  }

  test("No type") {
    val spec = extract("""{
                         |  recompute: true,
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (None)
        compStrategy.recompute should equal (Some(true))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))
      case _ => fail("didn't extract")
    }
  }

  test("Bad type") {
    val spec = extract("""{
                            type: "giraffe",
                         |  recompute: true,
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case RequestProblem(ComputationStrategySpecUnknownType(_)) =>
      case _ => fail("parsing should fail if type is invalid")
    }
  }

  test("No recompute") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(ComputationStrategyType.GeoRegionMatchOnPoint))
        compStrategy.recompute should be (None)
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))
      case _ => fail("parsing should fail if recompute is missing")
    }
  }

  test("No source columns") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  recompute: true,
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(ComputationStrategyType.GeoRegionMatchOnPoint))
        compStrategy.recompute should equal (Some(true))
        compStrategy.sourceColumns should be (None)
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))

      case _ => fail("didn't extract")
    }
  }

  test("No parameters") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  recompute: true,
                         |  source_columns: ["location"]
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(ComputationStrategyType.GeoRegionMatchOnPoint))
        compStrategy.recompute should equal (Some(true))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (None)

      case _ => fail("didn't extract")
    }
  }
}

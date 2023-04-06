package com.socrata.soda.server.services

import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.wiremodels.{Extracted, RequestProblem, UserProvidedComputationStrategySpec}
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast.{JObject, JString}
import com.socrata.soda.server.responses.ComputationStrategySpecUnknownType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ComputationStrategyExtractorTest extends AnyFunSuite with Matchers {
  def extract(input: String) = UserProvidedComputationStrategySpec.fromObject(JsonUtil.parseJson[JObject](input).right.get)

  test("All fields populated") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(StrategyType.GeoRegionMatchOnPoint))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))

      case _ => fail("didn't extract")
    }
  }

  test("No type") {
    val spec = extract("""{
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (None)
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))
      case _ => fail("didn't extract")
    }
  }

  test("Bad type") {
    val spec = extract("""{
                            type: "giraffe",
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case RequestProblem(ComputationStrategySpecUnknownType(_)) =>
      case _ => fail("parsing should fail if type is invalid")
    }
  }

  test("With legacy recompute") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  recompute: true,
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(StrategyType.GeoRegionMatchOnPoint))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))
      case _ => fail("parsing should not fail if recompute is provided")
    }
  }

  test("No source columns") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(StrategyType.GeoRegionMatchOnPoint))
        compStrategy.sourceColumns should be (None)
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))

      case _ => fail("didn't extract")
    }
  }

  test("No parameters") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  source_columns: ["location"]
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(StrategyType.GeoRegionMatchOnPoint))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (None)

      case _ => fail("didn't extract")
    }
  }

  test("Including depricated recompute field") {
    val spec = extract("""{
                         |  type: "georegion_match_on_point",
                         |  recompute: true,
                         |  source_columns: ["location"],
                         |  parameters: { georegion_uid:"abcd-1234" }
                         |}""".stripMargin)
    spec match {
      case Extracted(compStrategy) =>
        compStrategy.strategyType should be (Some(StrategyType.GeoRegionMatchOnPoint))
        compStrategy.sourceColumns should be (Some(Seq("location")))
        compStrategy.parameters should be (Some(JObject(Map("georegion_uid" -> JString("abcd-1234")))))

      case _ => fail("didn't extract")
    }
  }
}

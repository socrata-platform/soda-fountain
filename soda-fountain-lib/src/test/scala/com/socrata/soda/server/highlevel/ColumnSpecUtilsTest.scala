package com.socrata.soda.server.highlevel

import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.wiremodels.UserProvidedComputationStrategySpec
import org.scalatest.{FunSuite, Matchers}
import java.security.SecureRandom
import com.socrata.soql.environment.ColumnName

class ColumnSpecUtilsTest extends FunSuite with Matchers {
  lazy val rng = new scala.util.Random(new SecureRandom())
  lazy val columnSpecUtils = new ColumnSpecUtils(rng)

  def test(columnName: String, uCompStrategy: Option[UserProvidedComputationStrategySpec], expectValid: Boolean) =
    columnSpecUtils.validColumnName(ColumnName(columnName), uCompStrategy) should be (expectValid)

  test("validColumnName - non computed column, valid name") {
    test("address", None, true)
  }

  test("validColumnName - non computed column, :@ name") {
    test(":@address", None, true)
  }

  test("validColumnName - non computed column, blank name") {
    test("", None, false)
  }

  test("validColumnName - non computed column, invalid chars") {
    test("add,ress", None, false)
  }

  test("validColumnName - non computed column, underscore") {
    test("add_ress", None, true)
  }

  test("validColumnName - non computed column, hyphen") {
    test("add-ress", None, true)
  }

  test("validColumnName - non computed column, colon") {
    test(":address", None, false)
  }

  test("validColumnName - computed column, colon") {
    test(":location", Some(UserProvidedComputationStrategySpec(Some(StrategyType.Test), None, None)), true)
  }

  test("validColumnName - computed column, no colon allowed") {
    test("location", Some(UserProvidedComputationStrategySpec(Some(StrategyType.Geocoding), None, None)), true)
  }

  test("validColumnName - computed column, no colon not allowed") {
    test("region", Some(UserProvidedComputationStrategySpec(Some(StrategyType.GeoRegionMatchOnPoint), None, None)), false)
  }

  test("validColumnName - computed column, collides with system column") {
    test(":created_at",  Some(UserProvidedComputationStrategySpec(Some(StrategyType.Test), None, None)), false)
  }
}

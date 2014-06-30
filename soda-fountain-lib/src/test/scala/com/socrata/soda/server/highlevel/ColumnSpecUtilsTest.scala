package com.socrata.soda.server.highlevel

import org.scalatest.{FunSuite, Matchers}
import java.security.SecureRandom
import com.socrata.soql.environment.ColumnName

class ColumnSpecUtilsTest extends FunSuite with Matchers {
  lazy val rng = new scala.util.Random(new SecureRandom())
  lazy val columnSpecUtils = new ColumnSpecUtils(rng)

  def test(columnName: String, isComputed: Boolean, expectValid: Boolean) =
    columnSpecUtils.validColumnName(ColumnName(columnName), isComputed) should be (expectValid)

  test("validColumnName - non computed column, valid name") {
    test("address", false, true)
  }

  test("validColumnName - non computed column, :@ name") {
    test(":@address", false, true)
  }

  test("validColumnName - non computed column, blank name") {
    test("", false, false)
  }

  test("validColumnName - non computed column, invalid chars") {
    test("add,ress", false, false)
  }

  test("validColumnName - non computed column, underscore") {
    test("add_ress", false, true)
  }

  test("validColumnName - non computed column, hyphen") {
    test("add-ress", false, true)
  }

  test("validColumnName - non computed column, colon") {
    test(":address", false, false)
  }

  test("validColumnName - computed column, colon") {
    test(":location", true, true)
  }

  test("validColumnName - computed column, no colon") {
    test("location", true, false)
  }

  test("validColumnName - computed column, collides with system column") {
    test(":created_at", true, false)
  }
}

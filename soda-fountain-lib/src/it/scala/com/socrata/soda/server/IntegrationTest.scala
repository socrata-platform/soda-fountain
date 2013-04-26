package com.socrata.soda.server

import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

class IntegrationTest extends FunSuite with MustMatchers {

  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

  def jsonCompare(actual:String, expected:String) = {
    normalizeWhitespace(actual) must equal (normalizeWhitespace(expected))
  }
}

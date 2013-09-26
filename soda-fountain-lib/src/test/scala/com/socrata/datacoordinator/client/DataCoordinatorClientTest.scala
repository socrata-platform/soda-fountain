package com.socrata.datacoordinator.client

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.io.{CompactJsonWriter, JsonReader}

class DataCoordinatorClientTest extends FunSuite with MustMatchers {
  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())
}

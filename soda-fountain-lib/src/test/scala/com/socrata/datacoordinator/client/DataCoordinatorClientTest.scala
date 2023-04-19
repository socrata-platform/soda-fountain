package com.socrata.datacoordinator.client

import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class DataCoordinatorClientTest extends AnyFunSuite with Matchers {
  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())
}

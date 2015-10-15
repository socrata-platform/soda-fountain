package com.socrata.soda.server.resources

import org.scalatest.{Matchers, FunSuite}

class PartialArrayTest  extends FunSuite with Matchers {

  test("partial array") {
    val skipStartIndex = 1
    val skipLength = 4
    val array = (1 to 10).toArray

    val partialArray = new PartialArray(array, skipStartIndex, skipLength)
    val expected = array.take(skipStartIndex) ++ array.drop(skipStartIndex + skipLength)
    partialArray.length should be (expected.length)
    ( 0 until expected.length).foreach { i =>
      partialArray(i) should be (expected(i))
    }
  }
}

package com.socrata.datacoordinator.client

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite

class MutationScriptTest extends FunSuite with MustMatchers {

  test("toString produces JSON") {
    val ms = new MutationScript("test")

    ms.toString must equal ("{test:\"test\"")
  }

}

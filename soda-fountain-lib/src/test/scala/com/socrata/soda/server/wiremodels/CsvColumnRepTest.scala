package com.socrata.soda.server.wiremodels

import com.socrata.soql.types.SoQLType
import org.scalatest.{FunSuite, MustMatchers}

class CsvColumnRepTest extends FunSuite with MustMatchers {
  test("Reps know about all types") {
    CsvColumnRep.forType.keySet must equal (SoQLType.typesByName.values.toSet)
  }
}

package com.socrata.soda.external

import com.rojoma.json.v3.ast.JString
import com.socrata.soql.types.{SoQLNumber, SoQLType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers


class CsvColumnRepTest extends AnyFunSuite with Matchers {
  test("Reps know about all types") {
    CsvColumnRep.forType.keySet must equal (SoQLType.typesByName.values.toSet)
  }

  test("Number is written as plain text (without scientific notation)"){
    val input = SoQLNumber(BigDecimal(0.0000005302).bigDecimal)
    CsvColumnRep.forType(SoQLNumber).toString(input) must equal ("0.0000005302")
  }

  test("Really long number is written in scientific notation"){
    val input = SoQLNumber(BigDecimal(0.000000000000000000005302).bigDecimal)
    CsvColumnRep.forType(SoQLNumber).toString(input) must equal ("5.302E-21")
  }
}

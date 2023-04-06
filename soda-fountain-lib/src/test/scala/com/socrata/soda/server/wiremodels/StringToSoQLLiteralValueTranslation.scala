package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

/*
    Single-row operations send a column value rep in the URL, and this is plumbed into a SoQL query
 */

class StringToSoQLLiteralValueTranslation extends AnyFunSuite with Matchers {

  test("String reps know about all types") {
    pendingUntilFixed {
      StringColumnRep.forType.keySet must equal (SoQLType.typesByName.values.toSet)
    }
  }

  test("SoQL literal reps know about all types") {
    pendingUntilFixed {
      SoQLLiteralColumnRep.forType.keySet must equal (SoQLType.typesByName.values.toSet)
    }
  }

  def testRep(in :String, out :String, typ: SoQLType ){
    val inRep = StringColumnRep.forType(typ)
    val outRep = SoQLLiteralColumnRep.forType(typ)
    val inVal = inRep.fromString(in).getOrElse(throw new Exception("Rep did not produce value translating input"))
    val outVal = outRep.toSoQLLiteral(inVal)
    outVal must equal (out)
  }

  // would we ever want to specify a null value as a row id?
  //test("JSON type checker handles nulls"){ testRep("null", "null", SoQLText) }

  test("JSON rep type check with text"){ testRep("this is input", "((\"this is input\")::text)", SoQLText) }
  test("JSON rep type check with unicode text"){
    val input = "this is unicode input text   صص صꕥꔚꔄꔞഝആ"
    testRep(input, "((\"" + input + "\")::text)", SoQLText)
  }
  test("JSON rep type check with number"){ testRep("12345", "((12345)::number)", SoQLNumber) }
  test("JSON rep type check with boolean"){ testRep("t", "((true)::boolean)", SoQLBoolean) }
  test("JSON rep type check with double"){ pendingUntilFixed { testRep("123.45", "((123.45)::double)", SoQLDouble) } }
  test("JSON rep type check with money"){ testRep("123.45", "((123.45)::money)", SoQLMoney) }
  test("JSON rep type check with fixed timestamp"){ testRep("2013-06-03T02:26:05.123Z", "((\"2013-06-03T02:26:05.123Z\")::fixed_timestamp)", SoQLFixedTimestamp) }
  test("JSON rep type check with floating timestamp"){ testRep("2013-06-03T02:26:05.123", "((\"2013-06-03T02:26:05.123\")::floating_timestamp)", SoQLFloatingTimestamp) }
  test("JSON rep type check with date"){ pendingUntilFixed { testRep("2013-06-03", "((\"2013-06-03\")::date)", SoQLDate) } }
  test("JSON rep type check with time"){ pendingUntilFixed { testRep("02:26:05.123", "((\"02:26:05.123\")::time)", SoQLTime) } }
  //test("JSON rep type check with object"){ testRep("{a:1}", "{a:1}", SoQLObject) }
  //test("JSON rep type check with array"){ testRep("[1,2,3]", "[1,2,3]", SoQLArray) }
}

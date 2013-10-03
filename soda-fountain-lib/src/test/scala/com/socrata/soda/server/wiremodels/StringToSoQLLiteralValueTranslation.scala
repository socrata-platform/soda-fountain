package com.socrata.soda.server.wiremodels

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite
import com.socrata.soql.types._

/*
    Single-row operations send a column value rep in the URL, and this is plumbed into a SoQL query
 */

class StringToSoQLLiteralValueTranslation extends FunSuite with MustMatchers {

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

  test("JSON rep type check with text"){ testRep("this is input", "\"this is input\"", SoQLText) }
  test("JSON rep type check with unicode text"){
    val input = "this is unicode input text   صص صꕥꔚꔄꔞഝആ"
    testRep(input, '"' + input +'"', SoQLText)
  }
  test("JSON rep type check with number"){ testRep("12345", "12345", SoQLNumber) }
  test("JSON rep type check with boolean"){ testRep("t", "true", SoQLBoolean) }
  test("JSON rep type check with double"){ pendingUntilFixed { testRep("123.45", "123.45", SoQLDouble) } }
  test("JSON rep type check with money"){ pendingUntilFixed { testRep("123.45", "123.45", SoQLMoney) } }
  test("JSON rep type check with fixed timestamp"){ pendingUntilFixed { testRep("2013-06-03T02:26:05.123Z", "2013-06-03T02:26:05.123Z", SoQLFixedTimestamp) } }
  test("JSON rep type check with floating timestamp"){ pendingUntilFixed { testRep("2013-06-03T02:26:05.123", "2013-06-03T02:26:05.123", SoQLFloatingTimestamp) } }
  test("JSON rep type check with date"){ pendingUntilFixed { testRep("2013-06-03", "2013-06-03", SoQLDate) } }
  test("JSON rep type check with time"){ pendingUntilFixed { testRep("02:26:05.123", "02:26:05.123", SoQLTime) } }
  //test("JSON rep type check with object"){ testRep("{a:1}", "{a:1}", SoQLObject) }
  //test("JSON rep type check with array"){ testRep("[1,2,3]", "[1,2,3]", SoQLArray) }
}

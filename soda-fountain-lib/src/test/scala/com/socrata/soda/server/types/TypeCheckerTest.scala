package com.socrata.soda.server.types

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite
import com.socrata.soql.types._
import com.rojoma.json.ast._
import org.joda.time.{DateTime, DateTimeZone, LocalDate}

class TypeCheckerTest extends FunSuite with MustMatchers {
  test("encoders knows about all types") {
    TypeChecker.encoders.keys.toSet must equal (SoQLType.typesByName.values.toSet)
  }

  test("decoders knows about all types") {
    TypeChecker.decoders.keys.toSet must equal (SoQLType.typesByName.values.toSet)
  }

  test("Typechecker recognizes JString for SoQLText") {
    TypeChecker.check(SoQLText, JString("foo")) must equal (Right(SoQLText("foo")))
  }

  test("Typechecker rejects non-JString for SoQLText") {
    TypeChecker.check(SoQLText, JBoolean.canonicalFalse) must be a 'Left
    TypeChecker.check(SoQLText, JNumber(17)) must be a 'Left
    TypeChecker.check(SoQLText, JArray(Seq(JString("Hello")))) must be a 'Left
  }

  test("Typechecker recognizes date text") {
    TypeChecker.check(SoQLDate, JString(SoQLDate.StringRep(new LocalDate(0L, DateTimeZone.UTC)))) must be (Right(SoQLDate(new LocalDate(0L, DateTimeZone.UTC))))
  }

  test("Typechecker rejects malformatted date text") {
    TypeChecker.check(SoQLDate, JString("he-ll-oo")) must be a 'Left
  }

  test("Typechecker rejects non-strings for dates") {
    TypeChecker.check(SoQLDate, JBoolean.canonicalTrue) must be a 'Left
    TypeChecker.check(SoQLDate, JNumber(11001001)) must be a 'Left
    TypeChecker.check(SoQLDate, JObject(Map.empty)) must be a 'Left
  }

  test("Typechecker accepts strings containing numbers for numbers") {
    TypeChecker.check(SoQLNumber, JString("55")) must be (Right(SoQLNumber(new java.math.BigDecimal(55))))
  }

  test("Typechecker rejects strings containing non-numbers for numbers") {
    pendingUntilFixed {
      TypeChecker.check(SoQLNumber, JString("hello world")) must be a 'Left
    }
  }

  test("Typechecker accepts strings containing numbers for doubles") {
    TypeChecker.check(SoQLDouble, JString("55.2")) must be (Right(SoQLDouble(55.2)))
  }

  test("Typechecker rejects strings containing non-numbers for doubles") {
    pendingUntilFixed {
      TypeChecker.check(SoQLDouble, JString("hello world")) must be a 'Left
    }
  }

  test("Typechecker rejects strings containing NaN for doubles") {
    // Is this correct?  Or should we define the external-format of a
    // double containing a NaN to be the JSON string "NaN"?  In the
    // latter case the data-coordinator and encode need to be updated.
    pendingUntilFixed {
      TypeChecker.check(SoQLDouble, JString("NaN")) must be a 'Left
    }
  }

  test("Typechecker rejects strings containing infinites for doubles") {
    // Is this correct?  Or should we define the external-format of a
    // double containing an infinity to be the JSON string containing
    // "Infinity" or the like?  In the latter case the data-coordinator
    // and encode need to be updated.
    pendingUntilFixed {
      TypeChecker.check(SoQLDouble, JString("+Infinity")) must be a 'Left
      TypeChecker.check(SoQLDouble, JString("-Infinity")) must be a 'Left
      TypeChecker.check(SoQLDouble, JString("Infinity")) must be a 'Left
    }
  }

  test("Typechecker accepts strings containing numbers for money") {
    TypeChecker.check(SoQLMoney, JString("16.4")) must be (Right(SoQLMoney(new java.math.BigDecimal("16.4"))))
  }

  test("Typechecker rejects strings containing non-numbers for money") {
    pendingUntilFixed {
      TypeChecker.check(SoQLMoney, JString("hello world")) must be a 'Left
    }
  }

  test("Typechecker accepts numbers for fixed timestamps") {
    pendingUntilFixed {
      TypeChecker.check(SoQLFixedTimestamp, JNumber(0)) must equal (SoQLFixedTimestamp(new DateTime(0L)))
    }
  }

  test("Typecheck recognizes ID text") {
    TypeChecker.check(SoQLID, JString("row-aaaa-aaaa-aaaa")) must be a 'Right
  }

  test("Typecheck rejects non-ID text") {
    TypeChecker.check(SoQLID, JString("row-!!!!-!!!!-!!!!")) must be a 'Left
  }

  test("Typecheck recognizes version text") {
    TypeChecker.check(SoQLVersion, JString("rv-aaaa-aaaa-aaaa")) must be a 'Right
  }

  test("Typecheck rejects non-version text") {
    TypeChecker.check(SoQLVersion, JString("row-!!!!-!!!!-!!!!")) must be a 'Left
  }
}

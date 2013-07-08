package com.socrata.soda.server.services.soqltypes

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite
import com.socrata.soda.server.types.TypeChecker
import com.rojoma.json.ast._
import com.socrata.soql.types._
import org.joda.time.{LocalTime, LocalDate, LocalDateTime, DateTime}

class TypeFitterTest extends FunSuite with MustMatchers {

  test("JSON type checker handles nulls"){
    pendingUntilFixed{
      TypeChecker.check("text", JNull) match {
        case Right(v) => {}
        case Left(msg) => fail("type check failed for valid value")
      }
    }
  }

  test("JSON type checker with text"){
    val input = "this is input text"
    TypeChecker.check("text", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLText => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with unicode text"){
    val input = "this is unicode input text   صص صꕥꔚꔄꔞഝആ"
    TypeChecker.check("text", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLText => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker for text: invalid input - object") {
    TypeChecker.check("text", JObject(Map())) match {
      case Right(v) =>
        fail("type check failed for valid value")
      case Left(msg) => {}
    }
  }

  test("JSON type checker with number (as string)"){
    val input = "12345"
    pendingUntilFixed{
      TypeChecker.check("number", JString(input)) match {
        case Right(v) => v match {
          case t: SoQLNumber => t.value must equal (input)
          case _ => fail("received unexpected type")
        }
        case Left(msg) => fail("type check failed for valid value")
      }
    }
  }

  test("JSON type checker with number (as number)"){
    val input = BigDecimal(12345).bigDecimal
    TypeChecker.check("number", JNumber(input)) match {
      case Right(v) => v match {
        case t: SoQLNumber => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with double"){
    val input = 123.456789
    TypeChecker.check("double", JNumber(input)) match {
      case Right(v) => v match {
        case t: SoQLDouble => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with money"){
    val input = BigDecimal(123.45).bigDecimal
    TypeChecker.check("money", JNumber(input)) match {
      case Right(v) => v match {
        case t:SoQLMoney => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with location"){
    val lat = 45.0
    val lon = 50.0
    TypeChecker.check("location", JArray(Seq(JNumber(lat), JNumber(lon), JObject(Map())))) match {
      case Right(v) => v match {
        case t: SoQLLocation =>
          t.latitude must equal (lat)
          t.longitude must equal (lon)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }


  test("JSON type checker with boolean"){
    val input = false
    TypeChecker.check("boolean", JBoolean(input)) match {
      case Right(v) => v match {
        case t: SoQLBoolean => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with fixed timestamp"){
    val input = "2013-06-03T02:26:05.123Z"
    TypeChecker.check( "fixed_timestamp", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLFixedTimestamp => t.value must equal (new DateTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with floating timestamp"){
    val input = "2013-06-03T02:26:05.123"
    TypeChecker.check( "floating_timestamp", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLFloatingTimestamp => t.value must equal (new LocalDateTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with date"){
    val input = "2013-06-03"
    TypeChecker.check( "date", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLDate => t.value must equal (new LocalDate(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with time"){
    val input = "02:26:05.123"
    TypeChecker.check( "time", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLTime => t.value must equal (new LocalTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with invalid time"){
    val input = "@0z2:2!6:0$5.123"
    TypeChecker.check( "time", JString(input)) match {
      case Right(v) => v match {
        case _ => fail("invalid type should not have passed type check")
      }
      case Left(msg) => {}
    }
  }

  test("JSON type checker with array"){
    val input = Seq(JString("this is text"), JNumber(222), JNull, JBoolean(true))
    TypeChecker.check( "array", JArray(input)) match {
      case Right(v) => v match {
        case SoQLArray(JArray(t)) => t must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type checker with object"){
    val input = Map(("key" -> JString("value")))
    TypeChecker.check("object" ,JObject(input)) match {
      case Right(v) => v match {
        case SoQLObject(JObject(map)) => map must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }
}


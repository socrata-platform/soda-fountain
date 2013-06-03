package com.socrata.soda.server.services.soqltypes

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite
import com.socrata.soda.server.typefitting.TypeFitter
import com.rojoma.json.ast._
import com.socrata.soql.types._
import org.joda.time.DateTime

class TypeFitterTest extends FunSuite with MustMatchers {

  test("JSON type fitter handles nulls"){
    TypeFitter.check("text", JNull) match {
      case Right(v) => {}
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with text"){
    val input = "this is input text"
    TypeFitter.check("text", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLText => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with unicode text"){
    val input = "this is unicode input text   صص صꕥꔚꔄꔞഝആ"
    TypeFitter.check("text", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLText => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter for text: invalid input - object") {
    TypeFitter.check("text", JObject(Map())) match {
      case Right(v) =>
        fail("type check failed for valid value")
      case Left(msg) => {}
    }
  }

  test("JSON type fitter with number (as string)"){
    val input = "12345"
    TypeFitter.check("number", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLNumber => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with number (as number)"){
    val input = 12345
    TypeFitter.check("number", JNumber(input)) match {
      case Right(v) => v match {
        case t: SoQLNumber => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with double"){
    val input = 123.456789
    TypeFitter.check("double", JNumber(input)) match {
      case Right(v) => v match {
        case t: SoQLDouble => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with money"){
    val input = 123.45
    TypeFitter.check("money", JNumber(input)) match {
      case Right(v) => v match {
        case t:SoQLMoney => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with location"){
    val lat = 45.0
    val lon = 50.0
    TypeFitter.check("location", JArray(Seq(JNumber(lat), JNumber(lon), JObject(Map())))) match {
      case Right(v) => v match {
        case t: SoQLLocation =>
          t.latitude must equal (lat)
          t.longitude must equal (lon)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }


  test("JSON type fitter with boolean"){
    val input = false
    TypeFitter.check("boolean", JBoolean(input)) match {
      case Right(v) => v match {
        case t: SoQLBoolean => t.value must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with fixed timestamp"){
    val input = "2013-06-03T02:26:05.123Z"
    TypeFitter.check( "fixed_timestamp", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLFixedTimestamp => t.value must equal (new DateTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with floating timestamp"){
    val input = "2013-06-03T02:26:05.123"
    TypeFitter.check( "floating_timestamp", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLFixedTimestamp => t.value must equal (new DateTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with date"){
    val input = "2013-06-03"
    TypeFitter.check( "date", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLFixedTimestamp => t.value must equal (new DateTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with time"){
    val input = "02:26:05.123"
    TypeFitter.check( "time", JString(input)) match {
      case Right(v) => v match {
        case t: SoQLFixedTimestamp => t.value must equal (new DateTime(input))
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with array"){
    val input = Seq(JString("this is text"), JNumber(222), JNull, JBoolean(true))
    TypeFitter.check( "array", JArray(input)) match {
      case Right(v) => v match {
        case SoQLArray(JArray(t)) => t must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }

  test("JSON type fitter with object"){
    val input = Map(("key" -> JString("value")))
    TypeFitter.check("object" ,JObject(input)) match {
      case Right(v) => v match {
        case SoQLObject(JObject(map)) => map must equal (input)
        case _ => fail("received unexpected type")
      }
      case Left(msg) => fail("type check failed for valid value")
    }
  }
}


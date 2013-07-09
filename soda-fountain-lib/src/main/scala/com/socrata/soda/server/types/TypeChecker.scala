package com.socrata.soda.server.types

import com.socrata.soql.types._
import com.rojoma.json.ast._
import com.rojoma.json.ast.JString
import org.joda.time.{LocalDate, LocalTime, LocalDateTime, DateTime}

object TypeChecker {
  def check(typ: String, v: JValue) : Either[String, SoQLValue] = {
    def unexpected = Left("expecting " + SoQLText.toString() + " but received " + v.toString())

    //TODO: handle JNulls

    try {
      typ match {
        case "text" => v match { case JString(s) => Right(new SoQLText(s)); case _ => unexpected }
        case "number" => v match {
          case JString(n) => Right(new SoQLNumber(BigDecimal(n).bigDecimal))
          case JNumber(n) => Right(new SoQLNumber(n.bigDecimal))
          case _ => unexpected
        }
        case "boolean" => v match { case JBoolean(b) => Right(new SoQLBoolean(b)) ; case _ => unexpected }
        case "money" => v match {
          case JString(n) => Right(new SoQLMoney(BigDecimal(n).bigDecimal))
          case JNumber(n) => Right(new SoQLMoney(n.bigDecimal))
          case _ => unexpected
        }
        case "double" => v match {
          case JString(n) => Right(new SoQLDouble(n.toDouble))
          case JNumber(n) => Right(new SoQLDouble(n.toDouble))
          case _ => unexpected
        }
        case "fixed_timestamp" => v match {
          case JString(n) => Right(new SoQLFixedTimestamp(new DateTime(n)))
          case JNumber(n) => Right(new SoQLFixedTimestamp(new DateTime(n)))
          case _ => unexpected
        }
        case "floating_timestamp" => v match {
          case JString(n) => Right(new SoQLFloatingTimestamp(new LocalDateTime(n)))
          case _ => unexpected
        }
        case "date" => v match {
          case JString(n) => Right(new SoQLDate(new LocalDate(n)))
          case _ => unexpected
        }
        case "time" => v match {
          case JString(n) => Right(new SoQLTime(new LocalTime(n)))
          case _ => unexpected
        }
        case "object" => v match { case obj:JObject => Right(new SoQLObject(obj)); case _ => unexpected }
        case "array" => v match { case  arr:JArray =>  Right(new SoQLArray(arr)); case _ => unexpected }
        case "location" => v match {
          case JArray(arr) =>  arr match {
            case Seq(JNumber(lat), JNumber(lon), _) => Right(new SoQLLocation(lat.toDouble, lon.toDouble))
            case Seq(_, _, JObject(map)) => ???
            case _ => unexpected
          }
          case _ => unexpected
        }
        case "json" => v match { case obj: JObject =>  Right(new SoQLObject(obj)); case _ => unexpected }
        case _ => unexpected
      }
    } catch {
      case e: Exception => Left(e.getMessage)
    }
  }


  def encode( value: SoQLValue) : JValue = {
    value match {
      case num: SoQLNumber => JNumber(num.value)
      case str: SoQLText   => JString(str.value)

      case _ => JString(value.toString)
    }
  }
}

package com.socrata.soda.server.typefitting

import com.socrata.soql.types._
import com.rojoma.json.ast._
import com.rojoma.json.ast.JString
import org.joda.time.DateTime

object TypeFitter {
  def check(typ: String, v: JValue) = {
    def unexpected = throw new UnexpectedTypeException("expecting " + SoQLText.toString() + " but received " + v.toString())

    //TODO: handle JNulls

    val checked = typ match {
      case "text" => v match { case JString(s) => new SoQLText(s); case _ => unexpected }
      case "number" => v match {
        case JString(n) => new SoQLNumber(BigDecimal(n).bigDecimal)
        case JNumber(n) => new SoQLNumber(n.bigDecimal)
        case _ => unexpected
      }
      case "boolean" => v match { case JBoolean(b) => new SoQLBoolean(b) ; case _ => unexpected }
      case "money" => v match {
        case JString(n) => new SoQLMoney(BigDecimal(n).bigDecimal)
        case JNumber(n) => new SoQLMoney(n.bigDecimal)
        case _ => unexpected
      }
      case "double" => v match {
        case JString(n) => new SoQLDouble(n.toDouble)
        case JNumber(n) => new SoQLDouble(n.toDouble)
        case _ => unexpected
      }
      //TODO: implement these date/time type checks
      //case "fixed_timestamp" => v match {
      //  case JString(n) => new SoQLFixedTimestamp(new DateTime(n))
      //  case JNumber(n) => new SoQLFixedTimestamp(new DateTime(n))
      //  case _ => unexpected
      //}
      //case "floating_timestamp" => v match { case  =>  ; case _ => unexpected }
      //case "date" => v match { case  =>  ; case _ => unexpected }
      //case "time" => v match { case  =>  ; case _ => unexpected }
      case "object" => v match { case obj:JObject => new SoQLObject(obj); case _ => unexpected }
      case "array" => v match { case  arr:JArray =>  new SoQLArray(arr); case _ => unexpected }
      case "location" => v match {
        case JArray(arr) =>  arr match {
          case Seq(JNumber(lat), JNumber(lon), _) => new SoQLLocation(lat.toDouble, lon.toDouble)
          case Seq(_, _, JObject(map)) => ???
          case _ => unexpected
        }
        case _ => unexpected
      }
      case "json" => v match { case obj: JObject =>  new SoQLObject(obj); case _ => unexpected }
      case _ => unexpected
    }
    checked
  }

  class UnexpectedTypeException(msg: String) extends Exception(msg)
}

package com.socrata.soda.server.typefitting

import com.socrata.soql.types.{SoQLNumber, SoQLText, SoQLType}
import com.rojoma.json.ast.{JNumber, JString, JValue}

object TypeFitter {
  def check(typ: String, v: JValue) = {
    def unexpected = throw new UnexpectedTypeException("expecting " + SoQLText.toString() + " but received " + v.toString())
    val checked = typ match {
      case "text" => v match { case JString(s) => new SoQLText(s); case _ => unexpected }
      case "number" => v match {
        case JString(n) => new SoQLNumber(BigDecimal(n).bigDecimal)
        case JNumber(n) => new SoQLNumber(n.bigDecimal)
        case _ => unexpected
      }
      case _ => unexpected
    }
    checked
  }

  class UnexpectedTypeException(msg: String) extends Exception(msg)
}

package com.socrata.soda.server.id

import com.rojoma.json.ast.{JString, JValue}
import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.environment.AbstractName

class RollupName(s: String) extends AbstractName(s) {
  protected def hashCodeSeed: Int = 1562261336
}

object RollupName {
  implicit object RollupNameCodec extends JsonCodec[RollupName] {
    def encode(x: RollupName): JValue = JString(x.name)

    def decode(x: JValue): Option[RollupName] = x match {
      case JString(s) => Some(new RollupName(s))
      case _ => None
    }
  }
}

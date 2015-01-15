package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.codec.DecodeError.InvalidValue
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.socrata.soql.environment.AbstractName

class RollupName(s: String) extends AbstractName(s) {
  protected def hashCodeSeed: Int = 1562261336
}

object RollupName {
  implicit object RollupNameCodec extends JsonEncode[RollupName] with JsonDecode[RollupName] {
    def encode(x: RollupName): JValue = JString(x.name)

    def decode(x: JValue): DecodeResult[RollupName] = x match {
      case JString(s) => Right(new RollupName(s))
      case u => Left(InvalidValue(x))
    }
  }
}

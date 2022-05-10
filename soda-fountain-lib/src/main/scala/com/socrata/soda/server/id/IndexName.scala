package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.codec.DecodeError.InvalidValue
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.socrata.soql.environment.AbstractName

class IndexName(s: String) extends AbstractName(s) {
  protected def hashCodeSeed: Int = 1651528493
}

object IndexName {
  implicit object IndexNameCodec extends JsonEncode[IndexName] with JsonDecode[IndexName] {
    def encode(x: IndexName): JValue = JString(x.name)

    def decode(x: JValue): DecodeResult[IndexName] = x match {
      case JString(s) => Right(new IndexName(s))
      case u => Left(InvalidValue(x))
    }
  }
}

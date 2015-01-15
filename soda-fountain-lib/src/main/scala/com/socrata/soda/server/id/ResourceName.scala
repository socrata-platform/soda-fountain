package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.codec.DecodeError.InvalidType
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.socrata.soql.environment.AbstractName

class ResourceName(s: String) extends AbstractName(s) {
  protected def hashCodeSeed: Int = -795755684
}

object ResourceName {
  implicit object ResourceNameCodec extends JsonEncode[ResourceName] with JsonDecode[ResourceName] {
    def encode(x: ResourceName): JValue = JString(x.name)

    def decode(x: JValue): DecodeResult[ResourceName] = x match {
      case JString(s) => Right(new ResourceName(s))
      case u => Left(InvalidType(JString, u.jsonType))
    }
  }
}

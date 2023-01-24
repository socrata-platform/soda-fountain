package com.socrata.soda.server.util

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.socrata.soql.environment.ResourceName

object ResourceNameCodec {
  implicit val resourceNameJCodec = new JsonDecode[ResourceName] with JsonEncode[ResourceName] {
    def encode(resourceName: ResourceName) = JString(resourceName.name)

    def decode(v: JValue): Either[DecodeError, ResourceName] = v match {
      case n: JString => Right(ResourceName(n.string))
      case other => Left(DecodeError.InvalidType(JString, other.jsonType))

    }
  }
}

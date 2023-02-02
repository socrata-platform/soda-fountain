package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JNumber, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}

class CopyId(val underlying: Long) extends AnyVal {
  override def toString = s"CopyId($underlying)"
}

object CopyId {
  implicit val jCodec = new JsonDecode[CopyId] with JsonEncode[CopyId] {
    def encode(versionId: CopyId) = JNumber(versionId.underlying)
    def decode(v: JValue): Either[DecodeError, CopyId] = v match {
      case n: JNumber => Right(new CopyId(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }
}

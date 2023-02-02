package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JNumber, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}

class RollupMapId(val underlying: Long) extends AnyVal {
  override def toString = s"RollupMapId($underlying)"
}

object RollupMapId {
  implicit val jCodec = new JsonDecode[RollupMapId] with JsonEncode[RollupMapId] {
    def encode(versionId: RollupMapId) = JNumber(versionId.underlying)
    def decode(v: JValue): Either[DecodeError, RollupMapId] = v match {
      case n: JNumber => Right(new RollupMapId(n.toLong))
      case other      => Left(DecodeError.InvalidType(JNumber, other.jsonType))
    }
  }
}

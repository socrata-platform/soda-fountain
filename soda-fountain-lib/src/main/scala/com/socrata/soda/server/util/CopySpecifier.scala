package com.socrata.soda.server.util

import com.rojoma.json.v3.ast.{JNumber, JString, JValue}
import com.rojoma.json.v3.codec.{JsonEncode, DecodeError, JsonDecode}
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult

sealed abstract class CopySpecifier

object CopySpecifier {
  case object Latest extends CopySpecifier
  case object Published extends CopySpecifier
  case object Working extends CopySpecifier
  case class Snapshot(num: Long) extends CopySpecifier

  implicit object jCodec extends JsonDecode[CopySpecifier] with JsonEncode[CopySpecifier] {
    override def decode(x: JValue): DecodeResult[CopySpecifier] = x match {
      case JString("latest") => Right(Latest)
      case JString("published") => Right(Published)
      case JString("working") => Right(Working)
      case n: JNumber => Right(Snapshot(n.toLong))
      case _: JString | _ : JNumber => Left(DecodeError.InvalidValue(x))
      case _ => Left(DecodeError.Multiple(Seq(DecodeError.InvalidType(expected = JString, got = x.jsonType),
                                              DecodeError.InvalidType(expected = JNumber, got = x.jsonType))))
    }

    override def encode(x: CopySpecifier): JValue = x match {
      case Latest => JString("latest")
      case Published => JString("published")
      case Working => JString("working")
      case Snapshot(n) => JNumber(n)
    }
  }
}

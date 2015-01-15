package com.socrata.soda.server.copy

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.DecodeError.{InvalidValue, InvalidType}
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec._

sealed trait Stage {
  def name = this.toString
}

case object Unpublished extends Stage
case object Published extends Stage
// TODO: Not really handling snapshotted yet.
case object Snapshotted extends Stage
case object Discarded extends Stage
case object Latest extends Stage


object Stage {
  val InitialCopyNumber = 1L

  def apply(stage: String): Option[Stage] = {
    if (Option(stage).isEmpty) return Some(Latest)
    stage.toLowerCase match {
      case "unpublished" => Some(Unpublished)
      case "published"   => Some(Published)
      case "snapshotted" => Some(Snapshotted)
      case "discarded"   => Some(Discarded)
      case "latest"      => Some(Latest)
      case _             => None
    }
  }

  implicit val stageCodec = new JsonEncode[Stage] with JsonDecode[Stage] {
    def encode(x: Stage): JValue = JString(x.name)

    def decode(x: JValue): DecodeResult[Stage] = {
      x match {
        case JString(s) => Stage(s) match {
          case Some(stage) => Right(stage)
          case None => Left(InvalidValue(x))
        }
        case u => Left(InvalidType(JString, u.jsonType))
      }
    }
  }
}
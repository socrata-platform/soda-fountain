package com.socrata.soda.server.copy

import com.rojoma.json.ast.{JString, JValue}
import com.rojoma.json.codec.JsonCodec

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

  implicit val stageCodec = new JsonCodec[Stage] {
    def encode(x: Stage): JValue = JString(x.name)

    def decode(x: JValue): Option[Stage] = {
      x match {
        case JString(s) => Stage(s)
        case _ => None
      }
    }
  }
}
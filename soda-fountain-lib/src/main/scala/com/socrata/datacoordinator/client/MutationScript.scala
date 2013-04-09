package com.socrata.datacoordinator.client

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.rojoma.json.util.JsonUtil

object MutationScript {
  implicit val jCodec = new JsonCodec[MutationScript] {
    def encode(ms: MutationScript) = JObject(Map("script" -> JString(ms.value), "dup" -> JString(ms.value)))
    def decode(v:JValue): Option[MutationScript] = v match {
      case JObject(fields) =>
        fields.get("script") match {
          case Some(JString(s)) =>
            fields.get("dup") match {
              case Some(JString(d)) =>
                Some(new MutationScript(s))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }
}

class MutationScript(val value: String ) {
  override def toString = JsonUtil.renderJson(this)
}

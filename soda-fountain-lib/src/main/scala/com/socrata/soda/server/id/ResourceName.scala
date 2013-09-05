package com.socrata.soda.server.id

import com.socrata.soql.environment.AbstractName
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JString, JValue}

class ResourceName(s: String) extends AbstractName(s) {
  protected def hashCodeSeed: Int = -795755684
}

object ResourceName {
  implicit object ResourceNameCodec extends JsonCodec[ResourceName] {
    def encode(x: ResourceName): JValue = JString(x.name)

    def decode(x: JValue): Option[ResourceName] = x match {
      case JString(s) => Some(new ResourceName(s))
      case _ => None
    }
  }
}

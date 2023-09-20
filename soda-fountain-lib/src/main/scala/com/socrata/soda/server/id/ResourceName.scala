package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.codec.DecodeError.InvalidType
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.util.{WrapperFieldCodec, WrapperJsonCodec}
import com.socrata.soql.environment.AbstractName

class ResourceName(s: String) extends AbstractName(s) {
  protected def hashCodeSeed: Int = -795755684
}

object ResourceName {
  implicit val jCodec = WrapperJsonCodec[ResourceName](new ResourceName(_), _.name)
  implicit val fieldCodec = WrapperFieldCodec[ResourceName](new ResourceName(_), _.name)
}

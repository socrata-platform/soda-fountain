package com.socrata.soda.server.wiremodels

import com.rojoma.json.ast.{JValue, JString}
import com.rojoma.json.codec.JsonCodec
import scala.util.Try

object ComputationStrategyType extends Enumeration {
  implicit val JsonCodec = ComputationStrategyTypeCodec

  val GeoRegionMatchOnPoint = Value("georegion_match_on_point")
  val GeoRegionMatchOnString = Value("georegion_match_on_string")
  val Test      = Value("test")

  // For backwards compatibility. Superceded by georegion_match_on_point
  val GeoRegion = Value("georegion")
}

object ComputationStrategyTypeCodec extends JsonCodec[ComputationStrategyType.Value] {
  def encode(v: ComputationStrategyType.Value) = JString(v.toString)
  def decode(x: JValue) = x match {
    case JString(s) => Try(ComputationStrategyType.withName(s)).toOption
    case _ => None
  }
}
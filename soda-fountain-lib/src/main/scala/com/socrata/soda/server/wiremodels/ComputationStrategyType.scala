package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.DecodeError.{InvalidType, InvalidValue}
import com.rojoma.json.v3.codec._
import scala.util.Try

object ComputationStrategyType extends Enumeration {
  implicit val JsonCodec = ComputationStrategyTypeCodec

  val GeoRegionMatchOnPoint = Value("georegion_match_on_point")
  val GeoRegionMatchOnString = Value("georegion_match_on_string")
  val GeoCoding = Value("geocoding")
  val Test      = Value("test")

  // For backwards compatibility. Superceded by georegion_match_on_point
  val GeoRegion = Value("georegion")

  def userColumnAllowed(v: Value) = userColumnAllowedSet.contains(v)

  private val userColumnAllowedSet = Set(
    GeoCoding
  )

  // true if strategy type has a ComputationHandler in soda-fountain
  def computeSynchronously(v: Value) = computeSynchronouslySet.contains(v)

  private val computeSynchronouslySet = Set(
    GeoRegionMatchOnPoint,
    GeoRegionMatchOnString,
    Test
  )
}

object ComputationStrategyTypeCodec extends JsonEncode[ComputationStrategyType.Value] with JsonDecode[ComputationStrategyType.Value] {
  def encode(v: ComputationStrategyType.Value) = JString(v.toString)
  def decode(x: JValue) = x match {
    case JString(s) =>
      Try(ComputationStrategyType.withName(s)).toOption.map(Right(_)).getOrElse(Left(InvalidValue(x)))
    case u => Left(InvalidType(JString, u.jsonType))
  }
}
package com.socrata.soda.server.wiremodels

import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.types._
import com.rojoma.json.ast.{JNumber, JObject, JValue}

object LocationRep extends CodecBasedJsonColumnRep[SoQLLocation](SoQLLocation, _.asInstanceOf[SoQLLocation], _.asInstanceOf[SoQLLocation])(LocationCodec.codec)

object ClientLocationRep extends CodecBasedJsonColumnRep[SoQLLocation](SoQLLocation, _.asInstanceOf[SoQLLocation], _.asInstanceOf[SoQLLocation])(LocationCodec.clientCodec)

object LocationCodec {

  val codec = new LocationCodec("lat", "lon")
  val clientCodec = new LocationCodec("latitude", "longitude")
}

class LocationCodec(latitudeName: String, longitudeName: String) extends JsonCodec[SoQLLocation] {
  def encode(x: SoQLLocation): JValue =
    JObject(Map(
      latitudeName -> JNumber(x.latitude),
      longitudeName -> JNumber(x.longitude)
    ))

  def decode(x: JValue): Option[SoQLLocation] =
    x match {
      case JObject(fields) =>
        fields.get(latitudeName) match {
          case Some(JNumber(lat)) =>
            fields.get(longitudeName) match {
              case Some(JNumber(lon)) =>
                Some(SoQLLocation(lat.toDouble, lon.toDouble))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
}

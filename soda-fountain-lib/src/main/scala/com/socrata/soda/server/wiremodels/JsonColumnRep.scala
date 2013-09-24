package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.rojoma.json.ast.{JNumber, JString, JNull, JValue}
import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.types.obfuscation.CryptProvider

trait JsonColumnCommonRep {
  val representedType: SoQLType
}

trait JsonColumnReadRep extends JsonColumnCommonRep {
  def fromJValue(input: JValue): Option[SoQLValue]
}

trait JsonColumnWriteRep extends JsonColumnCommonRep {
  def toJValue(value: SoQLValue): JValue
  protected def stdBadValue: Nothing = sys.error("Incorrect value passed to toJValue")
}

trait JsonColumnRep extends JsonColumnReadRep with JsonColumnWriteRep

class CodecBasedJsonColumnRep[TrueCV : JsonCodec](val representedType: SoQLType, unwrapper: SoQLValue => TrueCV, wrapper: TrueCV => SoQLValue) extends JsonColumnRep {
  def fromJValue(input: JValue) =
    if(JNull == input) Some(SoQLNull)
    else JsonCodec[TrueCV].decode(input).map(wrapper)

  def toJValue(input: SoQLValue) =
    if(SoQLNull == input) JNull
    else JsonCodec[TrueCV].encode(unwrapper(input))
}

object JsonColumnRep {
  // this is used to en/decrypt row IDs and values.  The key DOESN'T MATTER,
  // because this system doesn't care about the actual values in a SoQLID or
  // a SoQLValue; it just cares that it can recognize and reproduce them.
  //
  // It would be better to not care about the decrypted values at all, but alas
  // that is not how SoQLID and SoQLVersion work.
  private[this] val cryptProvider = new CryptProvider(Array[Byte](0))
  val IdStringRep = new SoQLID.StringRep(cryptProvider)
  val VersionStringRep = new SoQLVersion.StringRep(cryptProvider)

  object TextRep extends CodecBasedJsonColumnRep[String](SoQLText, _.asInstanceOf[SoQLText].value, SoQLText(_))
  object NumberRep extends CodecBasedJsonColumnRep[java.math.BigDecimal](SoQLText, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_))
  object BooleanRep extends CodecBasedJsonColumnRep[Boolean](SoQLBoolean, _.asInstanceOf[SoQLBoolean].value, SoQLBoolean(_))

  object FixedTimestampRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(SoQLFixedTimestamp.StringRep(t)) => Some(SoQLFixedTimestamp(t))
      case JNull => Some(SoQLNull)
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value))

    val representedType: SoQLType = SoQLFixedTimestamp
  }

  object FloatingTimestampRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(SoQLFloatingTimestamp.StringRep(t)) => Some(SoQLFloatingTimestamp(t))
      case JNull => Some(SoQLNull)
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(SoQLFloatingTimestamp.StringRep(value.asInstanceOf[SoQLFloatingTimestamp].value))

    val representedType: SoQLType = SoQLFloatingTimestamp
  }

  object ClientNumberRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(s) => try { Some(SoQLNumber(new java.math.BigDecimal(s))) } catch { case e: NumberFormatException => None }
      case JNumber(n) => Some(SoQLNumber(n.underlying))
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(value.asInstanceOf[SoQLNumber].value.toString)

    val representedType: SoQLType = SoQLNumber
  }

  object IDRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(IdStringRep(id)) => Some(id)
      case JNull => Some(SoQLNull)
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(IdStringRep(value.asInstanceOf[SoQLID]))

    val representedType: SoQLType = SoQLID
  }

  object VersionRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(VersionStringRep(id)) => Some(id)
      case JNull => Some(SoQLNull)
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(VersionStringRep(value.asInstanceOf[SoQLVersion]))

    val representedType: SoQLType = SoQLVersion
  }

  def forClientType(typ: SoQLType): JsonColumnRep =
    typ match {
      case SoQLText => TextRep
      case SoQLFixedTimestamp => FixedTimestampRep
      case SoQLFloatingTimestamp => FloatingTimestampRep
      case SoQLID => IDRep
      case SoQLVersion => VersionRep
      case SoQLNumber => ClientNumberRep
      case SoQLBoolean => BooleanRep
    }

  def forDataCoordinatorType(typ: SoQLType): JsonColumnRep =
    typ match {
      case SoQLText => TextRep
      case SoQLFixedTimestamp => FixedTimestampRep
      case SoQLFloatingTimestamp => FloatingTimestampRep
      case SoQLID => IDRep
      case SoQLVersion => VersionRep
      case SoQLNumber => NumberRep
      case SoQLBoolean => BooleanRep
    }
}

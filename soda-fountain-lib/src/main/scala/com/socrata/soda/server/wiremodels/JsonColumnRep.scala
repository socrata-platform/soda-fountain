package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.rojoma.json.ast._
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
  object NumberRep extends CodecBasedJsonColumnRep[java.math.BigDecimal](SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_))
  object MoneyRep extends CodecBasedJsonColumnRep[java.math.BigDecimal](SoQLMoney, _.asInstanceOf[SoQLMoney].value, SoQLMoney(_))
  object BooleanRep extends CodecBasedJsonColumnRep[Boolean](SoQLBoolean, _.asInstanceOf[SoQLBoolean].value, SoQLBoolean(_))
  object ObjectRep extends CodecBasedJsonColumnRep[JObject](SoQLObject, _.asInstanceOf[SoQLObject].value, SoQLObject(_))
  object ArrayRep extends CodecBasedJsonColumnRep[JArray](SoQLArray, _.asInstanceOf[SoQLArray].value, SoQLArray(_))

  // Note: top-level `null's will be treated as SoQL nulls, not JSON nulls.  I think this is OK?
  object JValueRep extends CodecBasedJsonColumnRep[JValue](SoQLJson, _.asInstanceOf[SoQLJson].value, SoQLJson(_))

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

  object DateRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(SoQLDate.StringRep(t)) => Some(SoQLDate(t))
      case JNull => Some(SoQLNull)
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(SoQLDate.StringRep(value.asInstanceOf[SoQLDate].value))

    val representedType: SoQLType = SoQLDate
  }

  object TimeRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(SoQLTime.StringRep(t)) => Some(SoQLTime(t))
      case JNull => Some(SoQLNull)
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(SoQLTime.StringRep(value.asInstanceOf[SoQLTime].value))

    val representedType: SoQLType = SoQLTime
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

  object ClientMoneyRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JString(s) => try { Some(SoQLMoney(new java.math.BigDecimal(s))) } catch { case e: NumberFormatException => None }
      case JNumber(n) => Some(SoQLMoney(n.underlying))
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else JString(value.asInstanceOf[SoQLMoney].value.toString)

    val representedType: SoQLType = SoQLMoney
  }

  // Doubles are unquoted when we generate them, but we accept either quoted or unquoted for consistency.
  // Also NaN and the Infinites are represented as Strings.
  // We'll use this for both client and server doubles.  The servers will just never generate quoted ones.
  object DoubleRep extends JsonColumnRep {
    def fromJValue(input: JValue): Option[SoQLValue] = input match {
      case JNumber(n) => Some(SoQLDouble(n.toDouble))
      case JString(s) => try { Some(SoQLDouble(s.toDouble)) } catch { case e: NumberFormatException => None }
      case _ => None
    }

    def toJValue(value: SoQLValue): JValue =
      if(SoQLNull == value) JNull
      else {
        val v = value.asInstanceOf[SoQLDouble].value
        if(v.isInfinite || v.isNaN) JString(v.toString)
        else JNumber(v)
      }

    val representedType: SoQLType = SoQLDouble
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

  def forClientType: Map[SoQLType, JsonColumnRep] =
    Map(
      SoQLText -> TextRep,
      SoQLFixedTimestamp -> FixedTimestampRep,
      SoQLFloatingTimestamp -> FloatingTimestampRep,
      SoQLDate -> DateRep,
      SoQLTime -> TimeRep,
      SoQLID -> IDRep,
      SoQLVersion -> VersionRep,
      SoQLNumber -> ClientNumberRep,
      SoQLMoney -> ClientMoneyRep,
      SoQLDouble -> DoubleRep,
      SoQLBoolean -> BooleanRep,
      SoQLLocation -> ClientLocationRep,
      SoQLObject -> ObjectRep,
      SoQLArray -> ArrayRep,
      SoQLJson -> JValueRep
    )

  val forDataCoordinatorType: Map[SoQLType, JsonColumnRep] =
    Map(
      SoQLText -> TextRep,
      SoQLFixedTimestamp -> FixedTimestampRep,
      SoQLFloatingTimestamp -> FloatingTimestampRep,
      SoQLDate -> DateRep,
      SoQLTime -> TimeRep,
      SoQLID -> IDRep,
      SoQLVersion -> VersionRep,
      SoQLNumber -> NumberRep,
      SoQLMoney -> MoneyRep,
      SoQLDouble -> DoubleRep,
      SoQLBoolean -> BooleanRep,
      SoQLLocation -> LocationRep,
      SoQLObject -> ObjectRep,
      SoQLArray -> ArrayRep,
      SoQLJson -> JValueRep
    )
}

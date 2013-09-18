package com.socrata.soda.server.highlevel

import com.socrata.soql.types._
import com.rojoma.json.ast._
import com.rojoma.json.ast.JString
import org.joda.time.DateTime
import com.socrata.soda.server.wiremodels.JsonColumnRep

object TypeChecker {
  private[this] val IdStringRep = JsonColumnRep.IdStringRep
  private[this] val VersionStringRep = JsonColumnRep.VersionStringRep

  case class Error(expected: SoQLType, got: JValue)

  def check(typ: SoQLType, v: JValue) : Either[Error, SoQLValue] = v match {
    case JNull => Right(SoQLNull)
    case nonNull => decoders(typ).applyOrElse(nonNull, (e: JValue) => Left(Error(typ, e)))
  }

  // The functions return an Either so that they can produce errors
  // of their own, other than `unexpected`.  This isn't actually used
  // at the moment but I think it's a good idea.
  val decoders : Map[SoQLType, PartialFunction[JValue, Either[Error,SoQLValue]]] = Map(
    (SoQLNumber           , {
      case JString(n) => Right(new SoQLNumber(BigDecimal(n).bigDecimal)) // FIXME: NumberFormatException
      case JNumber(n) => Right(new SoQLNumber(n.bigDecimal))
    }),
    (SoQLDouble           , {
      case JString(n) => Right(new SoQLDouble(n.toDouble)) // FIXME: NumberFormatException, NaN, infinities
      case JNumber(n) => Right(new SoQLDouble(n.toDouble))
    }),
    (SoQLMoney            , {
      case JString(n) => Right(new SoQLMoney(BigDecimal(n).bigDecimal)) // FIXME: NumberFormatException
      case JNumber(n) => Right(new SoQLMoney(n.bigDecimal))
    }),
    (SoQLText             , { case JString(s) => Right(new SoQLText(s)) }),
    (SoQLObject           , { case obj: JObject => Right(new SoQLObject(obj)) }),
    (SoQLArray            , { case arr:JArray => Right(new SoQLArray(arr)) }),
    (SoQLLocation         , {
      case JArray(Seq(JNumber(lat), JNumber(lon), _)) =>
        Right(new SoQLLocation(lat.toDouble, lon.toDouble))
      case JArray(Seq(_, _, JObject(map))) =>
        ???
    }),
    (SoQLBoolean          , { case JBoolean(b) => Right(new SoQLBoolean(b)) }),
    (SoQLDate             , { case JString(SoQLDate.StringRep(t)) => Right(SoQLDate(t)) }),
    (SoQLTime             , { case JString(SoQLTime.StringRep(t)) => Right(SoQLTime(t)) }),
    (SoQLFixedTimestamp   , {
      case JString(SoQLFixedTimestamp.StringRep(t)) =>
        Right(SoQLFixedTimestamp(t))
      case JNumber(n) =>
        Right(new SoQLFixedTimestamp(new DateTime(n))) // FIXME: This will not work.  Yay typeless interfaces!
    }),
    (SoQLFloatingTimestamp, {
      case JString(SoQLFloatingTimestamp.StringRep(t)) =>
        Right(SoQLFloatingTimestamp(t))
    }),
    (SoQLID,                { case JString(IdStringRep(id)) => Right(id) }),
    (SoQLVersion,           { case JString(VersionStringRep(ver)) => Right(ver) }),
    (SoQLJson,              { case v => Right(SoQLJson(v)) })
  )

  val encoders : Map[SoQLType, SoQLValue => JValue] = Map(
    (SoQLNumber           , {num => JNumber(num.asInstanceOf[SoQLNumber].value)        }),
    (SoQLDouble           , {dub => JNumber(dub.asInstanceOf[SoQLDouble].value)        }),
    (SoQLMoney            , {mon => JNumber(mon.asInstanceOf[SoQLMoney].value)        }),
    (SoQLText             , {str => JString(str.asInstanceOf[SoQLText].value)        }),
    (SoQLObject           , {obj => obj.asInstanceOf[SoQLObject].value                 }),
    (SoQLArray            , {arr => arr.asInstanceOf[SoQLArray].value                 }),
    (SoQLLocation         , {loc => JArray(Seq(JNumber(loc.asInstanceOf[SoQLLocation].latitude), JNumber(loc.asInstanceOf[SoQLLocation].longitude)))}),
    (SoQLBoolean          , {boo => JBoolean(boo.asInstanceOf[SoQLBoolean].value)       }),
    (SoQLDate             , {dat => JString( SoQLDate.StringRep.apply( dat.asInstanceOf[SoQLDate].value))}),
    (SoQLTime             , {tim => JString( SoQLTime.StringRep.apply( tim.asInstanceOf[SoQLTime].value))}),
    (SoQLFixedTimestamp   , {xts => JString( SoQLFixedTimestamp.StringRep.apply( xts.asInstanceOf[SoQLFixedTimestamp].value))}),
    (SoQLFloatingTimestamp, {lts => JString( SoQLFloatingTimestamp.StringRep.apply( lts.asInstanceOf[SoQLFloatingTimestamp].value))}),
    (SoQLID               , {rid => JString(IdStringRep(rid.asInstanceOf[SoQLID]))}),
    (SoQLVersion          , {ver => JString(VersionStringRep(ver.asInstanceOf[SoQLVersion]))}),
    (SoQLJson             , {jsn => jsn.asInstanceOf[SoQLJson].value})
  )

  def encode(value: SoQLValue) : JValue =
    if(value eq SoQLNull) JNull
    else encoders(value.typ)(value) // flatMap{enc => enc(value)}

}

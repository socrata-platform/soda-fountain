package com.socrata.soda.server.types

import com.socrata.soql.types._
import com.rojoma.json.ast._
import com.rojoma.json.ast.JString
import org.joda.time.{LocalDate, LocalTime, LocalDateTime, DateTime}

object TypeChecker {

  def unexpected(v: JValue) = Left("expecting " + SoQLText.toString() + " but received " + v.toString())

  //TODO: handle JNulls
  def check(typ: SoQLType, v: JValue) : Either[String, SoQLValue] = { decoders.get(typ).get.apply(v) }


  val decoders : Map[SoQLType, JValue => Either[String,SoQLValue]] = Map(
    (SoQLNumber           , {v => v match {
          case JString(n) => Right(new SoQLNumber(BigDecimal(n).bigDecimal))
          case JNumber(n) => Right(new SoQLNumber(n.bigDecimal))
          case _ => unexpected(v)
        }}),
    (SoQLDouble           , {v =>v match {
          case JString(n) => Right(new SoQLDouble(n.toDouble))
          case JNumber(n) => Right(new SoQLDouble(n.toDouble))
          case _ => unexpected(v)
        }}),
    (SoQLMoney            , {v => v match {
          case JString(n) => Right(new SoQLMoney(BigDecimal(n).bigDecimal))
          case JNumber(n) => Right(new SoQLMoney(n.bigDecimal))
          case _ => unexpected(v)
        }}),
    (SoQLText             , {v => v match { case JString(s) => Right(new SoQLText(s)); case _ => unexpected(v) }}),
    (SoQLObject           , {v =>v match { case obj: JObject =>  Right(new SoQLObject(obj)); case _ => unexpected(v) }}),
    (SoQLArray            , {v => v match { case  arr:JArray =>  Right(new SoQLArray(arr)); case _ => unexpected(v) }}),
    (SoQLLocation         , {v => v match {
          case JArray(arr) =>  arr match {
            case Seq(JNumber(lat), JNumber(lon), _) => Right(new SoQLLocation(lat.toDouble, lon.toDouble))
            case Seq(_, _, JObject(map)) => ???
            case _ => unexpected(v)
          }
          case _ => unexpected(v)
        }}),
    (SoQLBoolean          , {v => v match { case JBoolean(b) => Right(new SoQLBoolean(b)) ; case _ => unexpected(v) }}),
    (SoQLDate             , {v => v match {
          case JString(n) => SoQLDate.StringRep.unapply(n) match {
            case Some(t) => Right(SoQLDate(t))
            case None => unexpected(v)
          }
          case _ => unexpected(v)
        }}),
    (SoQLTime             , {v => v match {
          case JString(n) => SoQLTime.StringRep.unapply(n) match {
            case Some(t) => Right(SoQLTime(t))
            case None => unexpected(v)
          }
          case _ => unexpected(v)
        }}),
    (SoQLFixedTimestamp   , {v => v match {
          case JString(n) => SoQLFixedTimestamp.StringRep.unapply(n) match {
            case Some(t) => Right(SoQLFixedTimestamp(t))
            case None => unexpected(v)
          }
          case JNumber(n) => Right(new SoQLFixedTimestamp(new DateTime(n)))
          case _ => unexpected(v)
        }}),
    (SoQLFloatingTimestamp, {v => v match {
          case JString(n) => SoQLFloatingTimestamp.StringRep.unapply(n) match {
            case Some(t) => Right(SoQLFloatingTimestamp(t))
            case None => unexpected(v)
          }
          case _ => unexpected(v)
        }})
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
    (SoQLFloatingTimestamp, {lts => JString( SoQLFloatingTimestamp.StringRep.apply( lts.asInstanceOf[SoQLFloatingTimestamp].value))})
  )

  def encode( value: SoQLValue) : JValue = encoders.get( value.typ ).get.apply(value) // flatMap{enc => enc(value)}

}

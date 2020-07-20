package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast._
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, ResourceName, RollupName}
import com.socrata.soql.environment.{ColumnName, TypeName}

trait Decoder[T] {
  def apply(v: JValue): Either[(String, JValue), T]
}

object Decoder {
  implicit val StringDecoder = new Decoder[String] {
    def apply(v: JValue) = v match {
      case JString(s) => Right(s)
      case _ => Left(("string", v))
    }
  }

  implicit val BooleanDecoder = new Decoder[Boolean] {
    def apply(v: JValue) = v match {
      case JBoolean(b) => Right(b)
      case _ => Left(("boolean", v))
    }
  }

  implicit val ResourceNameDecoder = new Decoder[ResourceName] {
    def apply(v: JValue) = v match {
      case JString(s) => Right(new ResourceName(s))
      case _ => Left(("string", v))
    }
  }

  implicit val ColumnNameDecoder = new Decoder[ColumnName] {
    def apply(v: JValue) = v match {
      case JString(s) => Right(new ColumnName(s))
      case _ => Left(("string", v))
    }
  }

  implicit val ColumnIdDecoder = new Decoder[ColumnId] {
    def apply(v: JValue) = v match {
      case JString(s) => Right(ColumnId(s))
      case _ => Left(("string", v))
    }
  }

  implicit val JArrayDecoder = new Decoder[JArray] {
    def apply(v: JValue) = v match {
      case arr: JArray => Right(arr)
      case _ => Left(("array", v))
    }
  }

  implicit val JObjectDecoder = new Decoder[JObject] {
    def apply(v: JValue) = v match {
      case obj: JObject => Right(obj)
      case _ => Left(("object", v))
    }
  }

  implicit val TypeNameDecoder = new Decoder[TypeName] {
    def apply(v: JValue) = v match {
      case JString(s) => Right(TypeName(s))
      case _ => Left(("string", v))
    }
  }

  implicit val RollupNameDecoder = new Decoder[RollupName] {
    def apply(v: JValue) = v match {
      case JString(s) => Right(new RollupName(s))
      case _ => Left(("string", v))
    }
  }

  implicit val StageDecoder = new Decoder[Stage] {
    def apply(v: JValue) = v match {
      case JString(s) =>
        Stage(Some(s)).map(Right(_)).getOrElse(Left(("stage", v)))
      case _ => Left(("stage", v))
    }
  }

  implicit def optionalThing[T](implicit d: Decoder[T])= new Decoder[Option[T]] {
    def apply(v: JValue) = v match {
      case JNull => Right(None)
      case _ => d(v).right.map(Some(_))
    }
  }

  implicit def sequenceOfThings[T](implicit d: Decoder[T]) = new Decoder[Seq[T]] {
    def apply(v: JValue): Either[(String, JValue), Seq[T]] = v match {
      case JArray(arr) =>
        val res = Vector.newBuilder[T]
        for(elem <- arr) {
          d(elem) match {
            case Right(x) => res += x
            case Left((expected, _)) => return Left((expected + " array", v))
          }
        }
        Right(res.result())
      case other =>
        Left(("array", other))
    }
  }
}

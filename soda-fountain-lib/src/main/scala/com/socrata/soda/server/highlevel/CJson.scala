package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast.{JArray, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.{JsonKey, Strategy, JsonKeyStrategy, AutomaticJsonCodecBuilder}
import com.socrata.soda.server.SodaInternalException
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.wiremodels.{JsonColumnReadRep, JsonColumnRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soda.server.util.AdditionalJsonCodecs._


import scala.runtime.AbstractFunction1

object CJson {
  case class Field(@JsonKey("c") columnId: ColumnId, @JsonKey("t") typ: SoQLType, @JsonKey("f") fieldName: Option[ColumnName])
  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  @JsonKeyStrategy(Strategy.Underscore)
  case class Schema(approximateRowCount: Option[Long], dataVersion: Option[Long], lastModified: Option[String], locale: String, pk: Option[ColumnId], rowCount: Option[Long], schema: Seq[Field])
  private implicit val schemaCodec = AutomaticJsonCodecBuilder[Schema]

  def decode(data: Iterator[JValue], jsonColumnReps: Map[SoQLType, JsonColumnRep]): Decoded = {
    if(!data.hasNext) throw NoSchemaPresent

    val schemaish = data.next()
    JsonDecode.fromJValue[Schema](schemaish) match {
      case Right(schema) =>
        class Processor extends AbstractFunction1[JValue, Array[SoQLValue]] {
          val reps = schema.schema.map { f => jsonColumnReps(f.typ) : JsonColumnReadRep }
          val width = reps.length
          def apply(row: JValue) = row match {
            case JArray(elems) =>
              if(elems.size != width) throw new RowIncorrectLength(elems.size, width)
              val result = new Array[SoQLValue](width)
              var i = 0
              while(i != width) {
                reps(i).fromJValue(elems(i)) match {
                  case Some(v) => result(i) = v
                  case None => throw new UndecodableValue(elems(i), schema.schema(i).typ)
                }
                i += 1
              }
              result
            case other =>
              throw new RowWasNotAnArray(other)
          }
        }
        Decoded(schema, data.map(new Processor))
      case Left(_) => throw new
          CannotDecodeSchema(schemaish)
    }
  }

  // CJSON SUCCESS
  case class Decoded(schema: Schema, rows: Iterator[Array[SoQLValue]])

  // If you want to handle failures then handle the exceptions, no need to have a result super class and handle
  // either success or failure casses.

  // EXCEPTIONS
  class CJsonException(msg: String) extends SodaInternalException(msg)

  case object NoSchemaPresent extends CJsonException("No Schema Present")
  case class CannotDecodeSchema(value: JValue) extends CJsonException(s"Could not decode Schema: $value")
  case class RowWasNotAnArray(value: JValue) extends CJsonException(s"Row was not an array: $value")
  case class RowIncorrectLength(got: Int, expected: Int)  extends CJsonException(s"Incorrect number of elements in row; expected $expected, got $got")
  case class UndecodableValue(got: JValue, expected: SoQLType)  extends CJsonException(s"Undecodable json value: ${got} (${got.jsonType}), " + s"expected SoQLType: ${expected.name.name}")
}

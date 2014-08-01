package com.socrata.soda.server.highlevel

import com.rojoma.json.ast.{JValue, JArray}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.{Strategy, JsonKeyStrategy, AutomaticJsonCodecBuilder}
import com.socrata.http.server.util.Precondition
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{ColumnRecordLike, NameAndSchemaStore}
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnReadRep}
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import scala.runtime.AbstractFunction1
import scala.util.control.ControlThrowable
import com.socrata.soda.server.util.schema.SchemaHash
import com.socrata.soda.server.copy.Published

object CJson {
  case class Field(c: ColumnId, t: SoQLType)
  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  @JsonKeyStrategy(Strategy.Underscore)
  case class Schema(approximateRowCount: Option[Long], dataVersion: Option[Long], lastModified: Option[String], locale: String, pk: Option[ColumnId], rowCount: Option[Long], schema: Seq[Field])
  private implicit val schemaCodec = AutomaticJsonCodecBuilder[Schema]

  def decode(data: Iterator[JValue]): Result = {
    if(!data.hasNext) return NoSchemaPresent
    val schemaish = data.next()
    JsonCodec.fromJValue[Schema](schemaish) match {
      case Some(schema) =>
        class Processor extends AbstractFunction1[JValue, Array[SoQLValue]] {
          val reps = schema.schema.map { f => JsonColumnRep.forDataCoordinatorType(f.t) : JsonColumnReadRep }
          val width = reps.length
          def apply(row: JValue) = row match {
            case JArray(elems) =>
              if(elems.size != width) throw new RowIncorrectLength(elems.size, width)
              val result = new Array[SoQLValue](width)
              var i = 0
              while(i != width) {
                reps(i).fromJValue(elems(i)) match {
                  case Some(v) => result(i) = v
                  case None => throw new UndecodableValue(elems(i), schema.schema(i).t)
                }
                i += 1
              }
              result
            case other =>
              throw new RowWasNotAnArray(other)
          }
        }
        Decoded(schema, data.map(new Processor))
      case None =>
        CannotDecodeSchema(schemaish)
    }
  }

  sealed abstract class Result
  case class Decoded(schema: Schema, rows: Iterator[Array[SoQLValue]]) extends Result
  case object NoSchemaPresent extends Result
  case class CannotDecodeSchema(value: JValue) extends Result

  case class RowWasNotAnArray(value: JValue) extends Exception("Row was not an array")
  case class RowIncorrectLength(got: Int, expected: Int) extends Exception(s"Incorrect number of elements in row; expected $expected, got $got")
  case class UndecodableValue(got: JValue, expected: SoQLType) extends Exception(s"")
}

class ExportDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient) extends ExportDAO {

  class Retry extends ControlThrowable

  val dateTimeParser = ISODateTimeFormat.dateTimeParser

  def retryable[T](limit: Int /* does not include the initial try */)(f: => T): T = {
    var count = 0
    var done = false
    var result: T = null.asInstanceOf[T]
    do {
      try {
        result = f
        done = true
      } catch {
        case _: Retry =>
          count += 1
          if(count > limit) throw new Exception("Retried too many times")
      }
    } while(!done)
    result
  }
  def retry() = throw new Retry

  def export[T](dataset: ResourceName,
                schemaCheck: Seq[ColumnRecordLike] => Boolean,
                onlyColumns: Seq[ColumnRecordLike],
                precondition: Precondition,
                ifModifiedSince: Option[DateTime],
                limit: Option[Long],
                offset: Option[Long],
                copy: String,
                sorted: Boolean)(f: ExportDAO.Result => T): T =
    retryable(limit = 5) {
      store.lookupDataset(dataset, Some(Published)) match { // TODO: Do we want to allow unpublished export?
        case Some(ds) =>
          if (schemaCheck(ds.columns)) {
            val schemaHash = onlyColumns match {
              case Seq() => ds.schemaHash
              case _     => SchemaHash.computeHash(ds.locale, ds.primaryKey, onlyColumns.map { col => (col.id, col.typ) })
            }
            val dcColumnIds = onlyColumns.map(_.id.underlying)
            dc.export(ds.systemId, schemaHash, dcColumnIds, precondition, ifModifiedSince, limit, offset, copy, sorted = sorted) {
              case DataCoordinatorClient.Export(jvalues, etag) =>
                CJson.decode(jvalues) match {
                  case CJson.Decoded(schema, rows) =>
                    val simpleSchema = ExportDAO.CSchema(
                      schema.approximateRowCount,
                      schema.dataVersion,
                      schema.lastModified.map(time => dateTimeParser.parseDateTime(time)),
                      schema.locale,
                      schema.pk.map(ds.columnsById(_).fieldName),
                      schema.rowCount,
                      schema.schema.map {
                        f => ColumnInfo(ds.columnsById(f.c).id, ds.columnsById(f.c).fieldName, ds.columnsById(f.c).name, f.t)
                      }
                    )
                    f(ExportDAO.Success(simpleSchema, etag, rows))
                }
              case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
                store.resolveSchemaInconsistency(ds.systemId, newSchema)
                retry()
              case DataCoordinatorClient.NotModified(etags) =>
                f(ExportDAO.NotModified(etags))
              case DataCoordinatorClient.PreconditionFailed =>
                f(ExportDAO.PreconditionFailed)
            }
          }
          else {
            f(ExportDAO.SchemaInvalidForMimeType)
          }
        case None =>
          f(ExportDAO.NotFound)
      }
    }
}

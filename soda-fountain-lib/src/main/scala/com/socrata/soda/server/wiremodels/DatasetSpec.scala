package com.socrata.soda.server.wiremodels

import scala.{collection => sc}


import InputUtils._
import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.errors.DatasetSpecMaltyped
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soql.environment.ColumnName

@JsonKeyStrategy(Strategy.Underscore)
case class DatasetSpec(resourceName: ResourceName,
                       name:String,
                       description: String,
                       rowIdentifier: ColumnName,
                       locale:String,
                       stage: Option[Stage],
                       columns:Map[ColumnName, ColumnSpec])
object DatasetSpec {
  private implicit val columnMapCodec = new JsonCodec[Map[ColumnName, ColumnSpec]] {
    def encode(x: Map[ColumnName, ColumnSpec]): JValue =
      JObject(x.map { case (k,v) => k.name -> JsonCodec.toJValue(v) })

    def decode(x: JValue): Option[Map[ColumnName, ColumnSpec]] = x match {
      case JObject(fields) =>
        val r = Map.newBuilder[ColumnName, ColumnSpec]
        fields foreach { case (k, v) =>
          JsonCodec.fromJValue[ColumnSpec](v) match {
            case Some(col) => r += new ColumnName(k) -> col
            case None => return None
          }
        }
        Some(r.result())
      case _ =>
        None
    }
  }

  implicit val jsonCodec = AutomaticJsonCodecBuilder[DatasetSpec]
}

case class UserProvidedDatasetSpec(resourceName: Option[ResourceName],
                                   name: Option[String],
                                   description: Option[String],
                                   rowIdentifier: Option[ColumnName],
                                   locale: Option[Option[String]],
                                   columns: Option[Seq[UserProvidedColumnSpec]])

object UserProvidedDatasetSpec extends UserProvidedSpec[UserProvidedDatasetSpec] {
  def fromObject(obj: JObject): ExtractResult[UserProvidedDatasetSpec] = {
    val dex = new DatasetExtractor(obj.fields)
    for {
      resourceName <- dex.resourceName
      name <- dex.name
      desc <- dex.description
      locale <- dex.locale
      rowId <- dex.rowId
      columns <- dex.columns
    } yield {
      UserProvidedDatasetSpec(resourceName, name, desc, rowId, locale, columns)
    }
  }

  // Using this class instead of AutomaticJsonCodecBuilder allows us to
  // return a specific SodaError citing what part of the extraction failed.
  private class DatasetExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(DatasetSpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def resourceName= e[ResourceName]("resource_name")
    def name = e[String]("name")
    def description = e[String]("description")
    def rowId = e[ColumnName]("row_identifier")
    def locale = e[Option[String]]("locale")
    def stage = e[Option[Stage]]("stage")
    def columns: ExtractResult[Option[Seq[UserProvidedColumnSpec]]] =
      e[Seq[JObject]]("columns") flatMap {
        case Some(arr) => ExtractResult.sequence(arr.map(UserProvidedColumnSpec.fromObject)).map(Some(_))
        case None => Extracted(None)
      }
  }
}

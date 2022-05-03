package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soda.server.responses.IndexSpecMaltyped

import com.socrata.soda.server.id.IndexName
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import scala.{collection => sc}

/**
 * Defines a Index.
 * @param name Client supplied string identifying the Index name.
 * @param expressions The soql expressions used to define the Index.
 * @param filter The where clause of the optional partial index
 */
@JsonKeyStrategy(Strategy.Underscore)
case class IndexSpec(name: IndexName, expressions: String, filter: Option[String])

object IndexSpec {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[IndexSpec]
}

case class UserProvidedIndexSpec(name: Option[IndexName], expressions: Option[String], filter: Option[String])

object UserProvidedIndexSpec extends UserProvidedSpec[UserProvidedIndexSpec] {
  def fromObject(obj: JObject) : ExtractResult[UserProvidedIndexSpec] = {
    val cex = new IndexExtractor(obj.fields)
    for {
      name <- cex.name
      expressions <- cex.expressions
      filter <- cex.filter
    } yield {
      UserProvidedIndexSpec(name, expressions, filter)
    }
  }

  // Using this class instead of AutomaticJsonCodecBuilder allows us to
  // return a specific SodaResponse citing what part of the extraction failed.
  class IndexExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(IndexSpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def name = e[IndexName]("name")
    def expressions = e[String]("expressions")
    def filter = e[String]("filter")
  }
}
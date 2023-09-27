package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.soda.server.responses.RollupSpecMaltyped
import com.socrata.soda.server.id.RollupName
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import com.socrata.soql.analyzer2.UnparsedFoundTables

import java.time.OffsetDateTime
import scala.{collection => sc}

/**
 * Defines a rollup.
 * @param name Client supplied string identifying the rollup name.
 * @param soql The soql query used to define the rollup.
 */
@JsonKeyStrategy(Strategy.Underscore)
case class RollupSpec(
  name: RollupName,
  soql: String,
  lastAccessed:Option[OffsetDateTime]=None)

object RollupSpec {
  import com.rojoma.json.v3.util.time.ISO8601.codec.offsetDateTimeCodec
  implicit val jsonCodec = AutomaticJsonCodecBuilder[RollupSpec]
}

case class UserProvidedRollupSpec(
  name: Option[RollupName],
  soql: Option[String])

object UserProvidedRollupSpec extends UserProvidedSpec[UserProvidedRollupSpec] {
  def fromObject(obj: JObject) : ExtractResult[UserProvidedRollupSpec] = {
    val cex = new RollupExtractor(obj.fields)
    for {
      name <- cex.name
      soql <- cex.soql
    } yield {
      UserProvidedRollupSpec(name, soql)
    }
  }

  // Using this class instead of AutomaticJsonCodecBuilder allows us to
  // return a specific SodaResponse citing what part of the extraction failed.
  class RollupExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(RollupSpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def name = e[RollupName]("name")
    def soql = e[String]("soql")
  }
}

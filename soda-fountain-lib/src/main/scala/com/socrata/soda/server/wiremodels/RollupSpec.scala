package com.socrata.soda.server.wiremodels

import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soda.server.errors.RollupSpecMaltyped

import com.socrata.soda.server.id.RollupName
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import scala.{collection => sc}


/**
 * Defines a rollup.
 * @param name Client supplied string identifying the rollup name.
 * @param soql The soql query used to define the rollup.
 */
@JsonKeyStrategy(Strategy.Underscore)
case class RollupSpec(
  name: RollupName,
  soql: String)

object RollupSpec {
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
  // return a specific SodaError citing what part of the extraction failed.
  class RollupExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(RollupSpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def name = e[RollupName]("name")
    def soql = e[String]("soql")
  }
}
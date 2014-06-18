package com.socrata.soda.server.wiremodels

import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soda.server.errors.{ComputationStrategySpecUnknownType, ComputationStrategySpecMaltyped}
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import scala.{collection => sc}
import scala.util.Try


/**
 * Defines how the value for a computed column is derived
 * @param strategyType Strategy used to compute the column value
 * @param recompute Whether the value should be recomputed every time the source columns are updated
 * @param sourceColumns Other columns in the dataset needed to compute the column value
 * @param parameters Additional custom parameters (expected contents depend on strategy type)
 */
@JsonKeyStrategy(Strategy.Underscore)
case class ComputationStrategySpec(
  strategyType: ComputationStrategyType.Value,
  recompute: Boolean,
  sourceColumns: Option[Seq[String]],
  parameters: Option[JObject])

object ComputationStrategySpec {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[ComputationStrategySpec]
}


case class UserProvidedComputationStrategySpec(strategyType: Option[ComputationStrategyType.Value],
                                               recompute: Option[Boolean],
                                               sourceColumns: Option[Seq[String]],
                                               parameters: Option[JObject])

object UserProvidedComputationStrategySpec extends UserProvidedSpec[UserProvidedComputationStrategySpec] {
  def fromObject(obj: JObject) : ExtractResult[UserProvidedComputationStrategySpec] = {
    val cex = new ComputationStrategyExtractor(obj.fields)
    for {
      strategyType <- cex.strategyType
      recompute <- cex.recompute
      sourceColumns <- cex.sourceColumns
      parameters <- cex.parameters
    } yield {
      UserProvidedComputationStrategySpec(strategyType, recompute, sourceColumns, parameters)
    }
  }

  // Using this class instead of AutomaticJsonCodecBuilder allows us to
  // return a specific SodaError citing what part of the extraction failed.
  class ComputationStrategyExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(ComputationStrategySpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def strategyType = e[String]("type") match {
      case Extracted(Some(s)) => Try(ComputationStrategyType.withName(s)).toOption match {
        case Some(typ) => Extracted(Some(typ))
        case None => RequestProblem(ComputationStrategySpecUnknownType(s))
      }
      case _ => Extracted(None)
    }
    def recompute = e[Boolean]("recompute")
    def sourceColumns = e[Seq[String]]("source_columns")
    def parameters = e[JObject]("parameters")
  }
}
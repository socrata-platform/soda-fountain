package com.socrata.soda.server.wiremodels

import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soda.server.errors.ComputationStrategySpecMaltyped
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import scala.{collection => sc}

@JsonKeyStrategy(Strategy.Underscore)
case class ComputationStrategySpec(strategyType: String, recompute: Boolean, sourceColumns: Seq[String], parameters: JObject)

object ComputationStrategySpec {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[ComputationStrategySpec]
}


case class UserProvidedComputationStrategySpec(strategyType: Option[String],
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

  class ComputationStrategyExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(ComputationStrategySpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def strategyType = e[String]("strategy_type")
    def recompute = e[Boolean]("recompute")
    def sourceColumns = e[Seq[String]]("source_columns")
    def parameters = e[JObject]("parameters")
  }
}
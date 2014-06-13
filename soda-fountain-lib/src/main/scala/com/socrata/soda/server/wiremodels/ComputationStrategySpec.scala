package com.socrata.soda.server.wiremodels

import scala.{collection => sc}
import com.rojoma.json.ast.{JArray, JObject, JValue}
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soda.server.errors.{ColumnSpecUnknownType, ComputationStrategySpecMaltyped}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.soql.types.SoQLType

@JsonKeyStrategy(Strategy.Underscore)
case class ComputationStrategySpec(strategyType: String, sourceColumns: Seq[String], parameters: JValue)

object ComputationStrategySpec {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[ComputationStrategySpec]
}


case class UserProvidedComputationStrategySpec(strategyType: Option[String],
                                               sourceColumns: Option[Seq[String]],
                                               parameters: Option[JValue])

object UserProvidedComputationStrategySpec extends UserProvidedSpec[UserProvidedComputationStrategySpec] {
  def fromObject(obj: JObject) : ExtractResult[UserProvidedComputationStrategySpec] = {
    val cex = new ComputationStrategyExtractor(obj.fields)
    for {
      strategyType <- cex.strategyType
      sourceColumns <- cex.sourceColumns
      parameters <- cex.parameters
    } yield {
      UserProvidedComputationStrategySpec(strategyType, sourceColumns, parameters)
    }
  }

  class ComputationStrategyExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(ComputationStrategySpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def strategyType = e[String]("strategy_type")
    def sourceColumns = e[Seq[String]]("source_columns")
    def parameters = e[JObject]("parameters")
  }
}
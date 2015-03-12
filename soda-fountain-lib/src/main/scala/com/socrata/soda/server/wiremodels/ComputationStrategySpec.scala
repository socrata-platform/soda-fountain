package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast.{JString, JObject, JValue}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.errors.{ComputationStrategySpecUnknownType, ComputationStrategySpecMaltyped}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.InputUtils.ExtractContext
import scala.{collection => sc}
import scala.util.Try
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult

@JsonKeyStrategy(Strategy.Underscore)
case class SourceColumnSpec(id: ColumnId,
                            fieldName: ColumnName)

object SourceColumnSpec {
  implicit object jsonCodec extends JsonEncode[SourceColumnSpec] with JsonDecode[SourceColumnSpec] {
    def encode(x: SourceColumnSpec): JValue = JString(x.fieldName.name)

    // have to extend JsonDecode for the auto-codecs below to work, but it's never actually used
    def decode(x: JValue): DecodeResult[SourceColumnSpec] =
     throw new NotImplementedError("decode not implemented")
  }
}

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
  sourceColumns: Option[Seq[SourceColumnSpec]],
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
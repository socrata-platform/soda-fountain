package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.util._
import com.rojoma.json.v3.ast._
import com.socrata.computation_strategies.{StrategyType, GeocodingParameterSchema, GeocodingSources}
import com.socrata.soda.server.wiremodels.{SourceColumnSpec, ComputationStrategySpec}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import com.socrata.soda.server.id.ColumnId

@JsonKeyStrategy(Strategy.Underscore)
case class ComputationStrategyInfo(strategyType: StrategyType, sourceColumnIds: Seq[ColumnId], parameters: JObject)

object ComputationStrategyInfo {

  def apply(spec: ComputationStrategySpec): ComputationStrategyInfo = {
    val ComputationStrategySpec(strategyType, optSourceColumns, parameters) = spec
    val sourceColumns = optSourceColumns.getOrElse(Seq.empty)
    ComputationStrategyInfo(
      strategyType,
      sourceColumns.map{ columnSpec => columnSpec.id },
      transform(strategyType, sourceColumns, parameters.getOrElse(JObject.canonicalEmpty))
    )
  }

  // TODO: does this want to move to the `computation_strategies` library?
  private def transform(typ: StrategyType, sourceColumns: Seq[SourceColumnSpec], parameters: JObject): JObject = {
    if (typ == StrategyType.Geocoding) {
      val sourceColumnIdMap = sourceColumns.map { col => (col.fieldName, col.id) }.toMap[ColumnName, ColumnId]
      implicit val decode = WrapperJsonDecode[ColumnName]{ name: String => new ColumnName(name): ColumnName }
      JsonDecode.fromJValue[GeocodingParameterSchema[ColumnName]](parameters) match {
        case Right(GeocodingParameterSchema(sources, defaults, version)) =>
          def lookupId(opt: Option[ColumnName]): Option[ColumnId] = opt match {
            case Some(fieldName) =>
              sourceColumnIdMap.get(fieldName) match {
                case some@Some(id) => some
                case None => throw new InternalError(s"Computation strategy parameters reference a column " +
                  s"not found in source columns after validation.")
              }
            case None => None
          }
          JsonEncode.toJValue[GeocodingParameterSchema[ColumnId]](
            GeocodingParameterSchema[ColumnId](
              sources =
                GeocodingSources[ColumnId](
                  address = lookupId(sources.address),
                  locality = lookupId(sources.locality),
                  subregion = lookupId(sources.subregion),
                  region = lookupId(sources.region),
                  postalCode = lookupId(sources.postalCode),
                  country = lookupId(sources.country)
                ),
              defaults = defaults,
              version = version
            )).asInstanceOf[JObject] // this really should be a JObject
        case Left(error) =>
          // this should not happen; we have already validated the computation strategy
          throw new InternalError(s"Failed to decode computation strategy parameters after valitation: ${error.english}")
      }
    } else {
      parameters
    }
  }

  implicit val jsonCodec = AutomaticJsonCodecBuilder[ComputationStrategyInfo]
}

sealed abstract class ColumnMutation extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class AddColumnInstruction(dataType: SoQLType, fieldName: ColumnName, id: Option[ColumnId], computationStrategy: Option[ComputationStrategySpec]) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("add column"),
    "field_name"  -> JString(fieldName.name),
    "type"  -> JString(dataType.name.name)) ++ id.map { cid =>
    "id" -> JString(cid.underlying)} ++ computationStrategy.map { strategy =>
    "computation_strategy" -> JsonEncode.toJValue[ComputationStrategyInfo](ComputationStrategyInfo(strategy))
  })
}

case class DropColumnInstruction(name: ColumnId) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("drop column"),
    "column"  -> JString(name.underlying)
  ))
}

case class SetRowIdColumnInstruction(name: ColumnId) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("set row id"),
    "column"  -> JString(name.underlying)
  ))
}

case class DropRowIdColumnInstruction(name: ColumnId) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("drop row id"),
    "column"  -> JString(name.underlying)
  ))
}

case class SetFieldNameInstruction(name: ColumnId, fieldName: ColumnName) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("update field name"),
    "column"  -> JString(name.underlying),
    "field_name"  -> JString(fieldName.name)
  ))
}

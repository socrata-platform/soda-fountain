package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy, JsonUtil}
import com.rojoma.json.v3.ast._
import com.socrata.soda.server.wiremodels.{ComputationStrategySpec, ComputationStrategyType}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import com.socrata.soda.server.id.ColumnId

@JsonKeyStrategy(Strategy.Underscore)
case class ComputationStrategyInfo(strategyType: ComputationStrategyType.Value, sourceColumnIds: Seq[ColumnId], parameters: JObject)

object ComputationStrategyInfo {

  def apply(spec: ComputationStrategySpec): ComputationStrategyInfo = {
    val ComputationStrategySpec(strategyType, sourceColumns, parameters) = spec
    ComputationStrategyInfo(
      strategyType,
      sourceColumns.getOrElse(Seq()).map{ columnSpec => columnSpec.id },
      parameters.getOrElse(JObject(Map()))
    )
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

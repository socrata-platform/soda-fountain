package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util._
import com.rojoma.json.v3.ast._
import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.wiremodels.ComputationStrategySpec
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
      parameters.getOrElse(JObject.canonicalEmpty)
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

case class AddComputationStrategyInstruction(name: ColumnId, strategy: ComputationStrategySpec) extends CM {
  def asJson = JObject(Map(
    "c"                    -> JString("add computation strategy"),
    "column"               -> JString(name.underlying),
    "computation_strategy" -> JsonEncode.toJValue[ComputationStrategyInfo](ComputationStrategyInfo(strategy))))
}

case class DropComputationStrategyInstruction(name: ColumnId) extends CM {
  def asJson = JObject(Map(
    "c"      -> JString("drop computation strategy"),
    "column" -> JString(name.underlying)))
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

case class SecondaryReindexInstruction() extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("secondary reindex")
  ))
}

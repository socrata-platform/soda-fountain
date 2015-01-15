package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast._
import com.socrata.soql.types.SoQLType
import com.socrata.soda.server.id.ColumnId

sealed abstract class ColumnMutation extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class AddColumnInstruction(dataType: SoQLType, hint: String, id: Option[ColumnId]) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("add column"),
    "hint"  -> JString(hint),
    "type"  -> JString(dataType.name.name)) ++ id.map { cid =>
    "id" -> JString(cid.underlying)
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

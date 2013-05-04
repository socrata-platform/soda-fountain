package com.socrata.datacoordinator.client


import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast.{JString, JObject}


sealed abstract class ColumnMutation extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class AddColumnInstruction(name: String, dataType: DatasetType) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("add column"),
    "name"  -> JString(name),
    "type"  -> JString(dataType.name)))
}

case class DropColumnInstruction(name: String) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("drop column"),
    "column"  -> JString(name)
  ))
}

case class RenameColumnInstruction(from: String, to: String) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("rename column"),
    "from"  -> JString(from),
    "to"    -> JString(to)))
}

case class SetRowIdColumnInstruction(name: String) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("set row id"),
    "column"  -> JString(name)
  ))
}

case class DropRowIdColumnInstruction(name: String) extends CM {
  def asJson = JObject(Map(
    "c"     -> JString("drop row id"),
    "column"  -> JString(name)
  ))
}

package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import com.socrata.soda.server.id.RollupName

sealed abstract class RollupMutation extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class CreateOrUpdateRollupInstruction(name: RollupName, soql: String) extends RollupMutation {
  def asJson = JObject(Map(
    "c"     -> JString("create or update rollup"),
    "name"  -> JString(name.name),
    "soql"  -> JString(soql)
  ))
}

case class DropRollupInstruction(name: RollupName) extends RollupMutation {
  def asJson = JObject(Map(
    "c"     -> JString("drop rollup"),
    "name"  -> JString(name.name)
  ))
}


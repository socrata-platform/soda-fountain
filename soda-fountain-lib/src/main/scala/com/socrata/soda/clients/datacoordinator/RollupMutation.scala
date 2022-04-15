package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soda.server.id.RollupName

sealed abstract class RollupMutation extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class CreateOrUpdateRollupInstruction(name: RollupName, soql: String, rawSoql: String) extends RollupMutation {
  def asJson = JObject(Map(
    "c"     -> JString("create or update rollup"),
    "name"  -> JString(name.name),
    "soql"  -> JString(soql),
    "raw_soql"  -> JString(rawSoql)
  ))
}

case class DropRollupInstruction(name: RollupName) extends RollupMutation {
  def asJson = JObject(Map(
    "c"     -> JString("drop rollup"),
    "name"  -> JString(name.name)
  ))
}


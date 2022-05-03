package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soda.server.id.IndexName

sealed abstract class IndexMutation extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class CreateOrUpdateIndexInstruction(name: IndexName, expressions: String, filter: Option[String]) extends IndexMutation {
  def asJson = JObject(Map(
    "c"     -> JString("create or update index"),
    "name"  -> JString(name.name),
    "expressions"  -> JString(expressions)
    ) ++ filter.map( x => "filter" -> JString(x)).toMap
  )
}

case class DropIndexInstruction(name: IndexName) extends IndexMutation {
  def asJson = JObject(Map(
    "c"     -> JString("drop index"),
    "name"  -> JString(name.name)
  ))
}


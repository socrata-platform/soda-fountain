package com.socrata.datacoordinator.client

import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast._
import com.rojoma.json.ast.JString

abstract class RowUpdateInstruction extends JsonEncodable {
  override def toString = JsonUtil.renderJson(asJson)
}

case class UpsertRowInstruction(rowData: Map[String, String]) extends RowUpdateInstruction {
  def asJson = JObject(rowData.mapValues(JString(_)))
}

case class DeleteRowInstruction(rowId: Either[String,BigDecimal]) extends RowUpdateInstruction {
  def asJson = JArray(Seq(rowId match {
    case Left(id) => JString(id)
    case Right(id) => JNumber(id)
  }))
}

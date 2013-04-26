package com.socrata.datacoordinator.client

import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast._

sealed abstract class RowUpdateInstruction extends JsonEncodable {
  override def toString = JsonUtil.renderJson(asJson)
}

case class UpsertRowInstruction(rowData: Map[String, JValue]) extends RowUpdateInstruction {
  def asJson = JObject(rowData)
}

case class DeleteRowInstruction(rowId: Either[String,BigDecimal]) extends RowUpdateInstruction {
  def asJson = JArray(Seq(rowId match {
    case Left(id) => JString(id)
    case Right(id) => JNumber(id)
  }))
}

case class RowUpdateOptionChange(truncate: Boolean = false,
                                 mergeInsteadOfReplace: Boolean = true,
                                 errorsAreFatal: Boolean = true) extends RowUpdateInstruction  {
//  def asJson = {
//    val map = scala.collection.mutable.Map[String, JValue]("c" -> JString("row data"))
//    if (truncate) map.put("truncate", JString(truncate.toString))
//    if (!mergeInsteadOfReplace) map.put("update", JString("replace"))
//    if (!errorsAreFatal) map.put("fatal_row_errors", JString(errorsAreFatal.toString))
//    JObject(map)
//  }

  def asJson = JObject(Map(
    "c" -> JString("row data"),
    "truncate" -> JBoolean(truncate),
    "update" -> (mergeInsteadOfReplace match {
      case true => JString("merge")
      case false => JString("replace")
    }),
    "fatal_row_errors" -> JBoolean(errorsAreFatal)
  ))
}

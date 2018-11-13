package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast._
import scala.collection.Map

sealed abstract class RowUpdate extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class UpsertRow(rowData: Map[String, JValue]) extends RowUpdate {
  def asJson = JObject(rowData)
}

case class DeleteRow(rowId: JValue) extends RowUpdate {
  def asJson = JArray(Seq(rowId))
}

case class RowUpdateOptionChange(truncate: Boolean = false,
                                 mergeInsteadOfReplace: Boolean = true,
                                 errorsAreFatal: Boolean = true,
                                 nonFatalRowErrors: Seq[String] = Seq())
  extends RowUpdate  {
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
    "fatal_row_errors" -> JBoolean(errorsAreFatal),
    "nonfatal_row_errors" -> JArray(nonFatalRowErrors.map(JString(_)))
  ))
}

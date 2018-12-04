package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast._
import scala.collection.Map
import com.socrata.http.server.HttpRequest
import scala.util.Try

sealed abstract class RowUpdate extends DataCoordinatorInstruction {
  override def toString = JsonUtil.renderJson(asJson)
}

case class UpsertRow(rowData: Map[String, JValue]) extends RowUpdate {
  def asJson = JObject(rowData)
}

case class DeleteRow(rowId: JValue) extends RowUpdate {
  def asJson = JArray(Seq(rowId))
}

// NOTE: If this class is changed, you may want to consider making similar changes
// to RowUpdateOption.java of the soda-java project
case class RowUpdateOptionChange(var truncate: Boolean = false,
                                 var mergeInsteadOfReplace: Boolean = true,
                                 var errorsAreFatal: Boolean = true,
                                 var nonFatalRowErrors: Seq[String] = Seq())
  extends RowUpdate {
    def updateFromReq(req: HttpRequest) {
      val parameterMap = req.servletRequest.getParameterMap()
      parameterMap.get("truncate") match {
        case null =>
        case trunc => this.truncate = Try(trunc.asInstanceOf[Array[String]].head.toBoolean).getOrElse(false)
      }
      parameterMap.get("mergeInsteadOfReplace") match {
        case null =>
        case mior => this.mergeInsteadOfReplace = Try(mior.asInstanceOf[Array[String]].head.toBoolean).getOrElse(true)
      }
      parameterMap.get("errorsAreFatal") match {
        case null =>
        case eaf => this.errorsAreFatal = Try(eaf.asInstanceOf[Array[String]].head.toBoolean).getOrElse(true)
      }
      parameterMap.get("nonFatalRowErrors") match {
        case null =>
        case nfre => this.nonFatalRowErrors = nfre.asInstanceOf[Array[String]]
      }
    }

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

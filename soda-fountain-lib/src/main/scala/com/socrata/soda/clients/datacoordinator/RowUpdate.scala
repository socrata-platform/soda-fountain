package com.socrata.soda.clients.datacoordinator

import scala.collection.JavaConverters._
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.http.server.{HttpRequest, ParsedParam, UnparsableParam}

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
case class RowUpdateOption(truncate: Boolean,
                           mergeInsteadOfReplace: Boolean,
                           errorPolicy: RowUpdateOption.ErrorPolicy)
  extends RowUpdate {

  def asJson = JObject(Map(
    "c" -> JString("row data"),
    "truncate" -> JBoolean(truncate),
    "update" -> (mergeInsteadOfReplace match {
      case true => JString("merge")
      case false => JString("replace")
    })
  ) + errorPolicy.fold("fatal_row_errors" -> JsonEncode.toJValue(false)) { nonFatalRowErrors =>
    "nonfatal_row_errors" -> JsonEncode.toJValue(nonFatalRowErrors)
  })
}

// Note: nonfatal_row_errors = [] implies fatal_row_errors = true unless otherwise set
object RowUpdateOption {
  val default = RowUpdateOption(
    truncate = false,
    mergeInsteadOfReplace = true,
    errorPolicy = NonFatalRowErrors(Nil)
  )

  sealed abstract class ErrorPolicy {
    def fold[T](noFatal: T)(nonFatals: Seq[String] => T): T
  }
  case object NoRowErrorsAreFatal extends ErrorPolicy {
    def fold[T](noFatal: T)(nonFatals: Seq[String] => T) = noFatal
  }
  case class NonFatalRowErrors(errors: Seq[String]) extends ErrorPolicy {
    def fold[T](noFatal: T)(nonFatals: Seq[String] => T) = nonFatals(errors)
  }

  def fromReq(req: HttpRequest): Either[(String, String), RowUpdateOption] = {
    def boolParam(name: String, default: Boolean) = {
      req.parseQueryParameterAs[Boolean](name) match {
        case ParsedParam(Some(b)) => Right(b)
        case ParsedParam(None) => Right(default)
        case UnparsableParam(_, value) => Left((name, value))
      }
    }

    def errorPolicyParam() = {
      val interesting = req.queryParametersSeq.collect {
        case ("nonFatalRowErrors[]", Some(value)) => value
      }
      if(interesting.nonEmpty) {
        Right(NonFatalRowErrors(interesting))
      } else {
        boolParam("errorsAreFatal", true).right.map {
          case true => NonFatalRowErrors(Nil)
          case false => NoRowErrorsAreFatal
        }
      }
    }

    for {
      truncate <- boolParam("truncate", false).right
      mergeInsteadOfReplace <- boolParam("mergeInsteadOfReplace", true).right
      errorPolicy <- errorPolicyParam().right
    } yield {
      RowUpdateOption(
        truncate = truncate,
        mergeInsteadOfReplace = mergeInsteadOfReplace,
        errorPolicy = errorPolicy
      )
    }
  }
}

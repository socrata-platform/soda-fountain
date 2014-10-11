package com.socrata.soda.clients.querycoordinator

import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.JValue
import com.socrata.http.server.util.{Precondition, EntityTag}
import org.joda.time.DateTime
import com.socrata.soda.server.copy.Stage

object QueryCoordinatorClient {
  sealed abstract class Result
  case class Success(etags: Seq[EntityTag], rollup: Option[String], response: Iterator[JValue]) extends Result
  case class NotModified(etags: Seq[EntityTag]) extends Result
  case object PreconditionFailed extends Result
  case class UserError(resultCode: Int, response: JValue) extends Result

  val HeaderRollup = "X-SODA2-Rollup"
}

trait QueryCoordinatorClient {
  import QueryCoordinatorClient._
  def query[T](datasetId: DatasetId, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String, columnIdMap: Map[ColumnName, ColumnId],
    rowCount: Option[String], copy: Option[Stage], secondaryInstance: Option[String], noRollup: Boolean)(f: Result => T): T
}

package com.socrata.soda.clients.querycoordinator

import com.rojoma.json.v3.ast.JValue
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

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
  def query[T](datasetId: DatasetId,
               precondition: Precondition,
               ifModifiedSince: Option[DateTime],
               query: String,
               columnIdMap: Map[ColumnName, ColumnId],
               rowCount: Option[String],
               copy: Option[Stage],
               secondaryInstance: Option[String],
               noRollup: Boolean,
               extraHeaders: Map[String, String],
               rs: ResourceScope)(f: Result => T): T
}

package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.{RequestId, Precondition}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence.{DatasetRecord, ColumnRecordLike}
import org.joda.time.DateTime

/**
 * Cannot mock ExportDAO because it contains functions with more than 9 arguments.
 */
object DummyExportDAO extends ExportDAO {
  def export[T](dataset: ResourceName,
                schemaCheck: Seq[ColumnRecordLike] => Boolean,
                onlyColumns: Seq[ColumnRecordLike],
                precondition: Precondition,
                ifModifiedSince: Option[DateTime],
                limit: Option[Long],
                offset: Option[Long],
                copy: String,
                sorted: Boolean,
                rowId: Option[String],
                requestId: RequestId.RequestId)(f: ExportDAO.Result => T): T = ???

  def lookupDataset(resourceName: ResourceName, copy: Option[Stage]): Option[DatasetRecord] = ???
}
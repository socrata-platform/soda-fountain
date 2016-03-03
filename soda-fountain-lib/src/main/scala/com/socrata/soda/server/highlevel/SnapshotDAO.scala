package com.socrata.soda.server.highlevel

import java.io.Reader

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.id.ResourceName

trait SnapshotDAO {
  def datasetsWithSnapshots(): Set[ResourceName]
  def snapshotsForDataset(resourceName: ResourceName): Option[Seq[Long]]
  def exportSnapshot(resourceName: ResourceName, snapshot: Long, resourceScope: ResourceScope): SnapshotDAO.ExportSnapshotResponse
  def deleteSnapshot(resourceName: ResourceName, snapshot: Long): SnapshotDAO.DeleteSnapshotResponse
}

object SnapshotDAO {
  sealed trait ExportSnapshotResponse
  sealed trait DeleteSnapshotResponse

  case object DatasetNotFound extends ExportSnapshotResponse with DeleteSnapshotResponse
  case object SnapshotNotFound extends ExportSnapshotResponse with DeleteSnapshotResponse
  case class Export(data: HttpResponse) extends ExportSnapshotResponse
  case object Deleted extends DeleteSnapshotResponse
}

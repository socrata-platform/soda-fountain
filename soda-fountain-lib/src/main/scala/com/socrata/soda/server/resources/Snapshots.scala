package com.socrata.soda.server.resources

import com.socrata.http.server.HttpRequest
import com.socrata.soda.server.{SodaUtils, SodaRequest}
import com.socrata.soda.server.responses.{SnapshotNotFound, DatasetNotFound}
import com.socrata.soda.server.highlevel.SnapshotDAO
import com.socrata.soda.server.id.ResourceName
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import org.apache.commons.io.IOUtils

case class Snapshots(snapshotDAO: SnapshotDAO) {
  val findDatasetsService = new SodaResource {
    override val get = { (req: SodaRequest) =>
      OK ~> Json(snapshotDAO.datasetsWithSnapshots())
    }
  }

  def listSnapshotsService(resourceName: ResourceName) = new SodaResource {
    override def get = { (req: SodaRequest) =>
      snapshotDAO.snapshotsForDataset(resourceName) match {
        case Some(snapshots) =>
          OK ~> Json(snapshots)
        case None =>
          SodaUtils.response(req, DatasetNotFound(resourceName))
      }
    }
  }

  def snapshotsService(resourceName: ResourceName, number: Long) = new SodaResource {
    override def get = { (req: SodaRequest) =>
      snapshotDAO.exportSnapshot(resourceName, number, req.resourceScope) match {
        case SnapshotDAO.DatasetNotFound =>
          SodaUtils.response(req, DatasetNotFound(resourceName))
        case SnapshotDAO.SnapshotNotFound =>
          SodaUtils.response(req, SnapshotNotFound(resourceName, number))
        case SnapshotDAO.Export(csv) =>
          OK ~> csv
      }
    }

    override def delete = { (req: SodaRequest) =>
      snapshotDAO.deleteSnapshot(resourceName, number) match {
        case SnapshotDAO.DatasetNotFound =>
          SodaUtils.response(req, DatasetNotFound(resourceName))
        case SnapshotDAO.SnapshotNotFound =>
          SodaUtils.response(req, SnapshotNotFound(resourceName, number))
        case SnapshotDAO.Deleted =>
          OK
      }
    }
  }
}

package com.socrata.soda.server.highlevel

import java.nio.charset.StandardCharsets

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, NoSuchDataset, NoSuchSnapshot}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{DatasetNotFoundResult, ExportResult, SnapshotNotFoundResult}
import com.socrata.soda.message.NoOpMessageProducer
import com.socrata.soda.server.export.CsvExporter
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLValue

class SnapshotDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient) extends SnapshotDAO {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SnapshotDAOImpl])

  override def datasetsWithSnapshots(): Set[ResourceName] =
    store.bulkDatasetLookup(dc.datasetsWithSnapshots())

  override def snapshotsForDataset(resourceName: ResourceName): Option[Seq[Long]] =
    store.lookupDataset(resourceName) match {
      case dss if dss.nonEmpty =>
        val ds = dss.head
        dc.listSnapshots(ds.systemId)
      case _ =>
        None
    }

  override def deleteSnapshot(resourceName: ResourceName, snapshot: Long): SnapshotDAO.DeleteSnapshotResponse =
    store.lookupDataset(resourceName) match {
      case dss if dss.nonEmpty =>
        val ds = dss.head
        dc.deleteSnapshot(ds.systemId, snapshot) match {
          case Right(_) =>
            SnapshotDAO.Deleted
          case Left(DataCoordinatorClient.SnapshotNotFoundResult(_, _)) =>
            SnapshotDAO.SnapshotNotFound
          case Left(DataCoordinatorClient.DatasetNotFoundResult(_)) =>
            SnapshotDAO.DatasetNotFound
          case Left(other) =>
            throw new Exception("Unexpected result from snapshot delete: " + other)
        }
      case _ =>
        SnapshotDAO.DatasetNotFound
    }

  override def exportSnapshot(resourceName: ResourceName, snapshot: Long, resourceScope: ResourceScope): SnapshotDAO.ExportSnapshotResponse = {
    store.lookupDataset(resourceName) match {
      case dss if dss.nonEmpty =>
        val ds = dss.head
        dc.exportSimple(ds.systemId, snapshot.toString, resourceScope) match {
          case ExportResult(json, _, _) =>
            val decodedSchema = CJson.decode(json, JsonColumnRep.forDataCoordinatorType)
            val schema = decodedSchema.schema
            val toKeep = Array[Boolean](schema.schema.map { f => !f.fieldName.fold(f.columnId.underlying)(_.name).startsWith(":") } : _*)
            val mappedRows = decodedSchema.rows.map { r =>
              (r, toKeep).zipped.collect { case (v, true) => v }.toArray
            }
            SnapshotDAO.Export(
              CsvExporter.export(
                AliasedCharset(StandardCharsets.UTF_8, "utf-8"),
                ExportDAO.CSchema(
                  approximateRowCount = None,
                  dataVersion = None,
                  lastModified = schema.lastModified.map(time => ExportDAO.dateTimeParser.parseDateTime(time)),
                  locale = schema.locale,
                  pk = None,
                  rowCount = None,
                  schema = schema.schema.map { f =>
                    val fieldName = f.fieldName.getOrElse(ColumnName(f.columnId.underlying))
                    ColumnInfo(f.columnId, fieldName, f.typ, None)
                  }.zip(toKeep).collect { case (v, true) => v }),
                mappedRows)(NoOpMessageProducer))
          case SnapshotNotFoundResult(_, _) =>
            SnapshotDAO.SnapshotNotFound
          case DatasetNotFoundResult(_) =>
            SnapshotDAO.DatasetNotFound
          case other =>
            log.error("Unexpected result from simple export: {}", other)
            throw new Exception("Unexpected result from simple export: " + other)
        }
      case _ =>
        SnapshotDAO.DatasetNotFound
    }
  }
}

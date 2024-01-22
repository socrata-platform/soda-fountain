package com.socrata.soda.server.util

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName

import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.server.id.{DatasetInternalName, ResourceName, ColumnId}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient

object DAOCache {
  sealed abstract class Error
  case class UnknownTable(table: DatabaseTableName[(ResourceName, Stage)]) extends Error
  case class UnknownColumn(table: DatabaseTableName[(ResourceName, Stage)], column: DatabaseColumnName[ColumnName]) extends Error
}

class DAOCache(store: NameAndSchemaStore) {
  import DAOCache._

  case class RelevantBits(
    resourceName: ResourceName,
    internalName: DatasetInternalName,
    stage: Stage,
    schema: Map[ColumnName, ColumnId],
    truthDataVersion: Long
  )

  private val datasetCache = new scm.HashMap[(ResourceName, Stage), RelevantBits]

  private def getRecord(dtn: DatabaseTableName[(ResourceName, Stage)]): Either[UnknownTable, RelevantBits] = {
    val DatabaseTableName(name@(rn, stage)) = dtn
    datasetCache.get(name) match {
      case Some(bits) =>
        Right(bits)
      case None =>
        store.lookupDataset(rn, Some(stage)) match {
          case Some(record) =>
            val bits =
              RelevantBits(
                rn,
                record.systemId,
                stage,
                record.columns.iterator.map { col =>
                  col.fieldName -> col.id
                }.toMap,
                record.truthVersion
              )
            datasetCache += name -> bits
            Right(bits)
          case None =>
            Left(UnknownTable(dtn))
        }
    }
  }

  def allTables: Iterator[RelevantBits] =
    datasetCache.values.toVector.iterator

  def lookupTableName(dtn: DatabaseTableName[(ResourceName, Stage)]): Either[UnknownTable, DatabaseTableName[(DatasetInternalName, Stage)]] = {
    getRecord(dtn).map { record =>
      DatabaseTableName((record.internalName, record.stage))
    }
  }

  def lookupColumnName(dtn: DatabaseTableName[(ResourceName, Stage)], dcn: DatabaseColumnName[ColumnName]): Either[Error, DatabaseColumnName[ColumnId]] = {
    val DatabaseColumnName(cn) = dcn
    getRecord(dtn).flatMap { record =>
      record.schema.get(cn) match {
        case Some(id) => Right(DatabaseColumnName(id))
        case None => Left(UnknownColumn(dtn, dcn))
      }
    }
  }

  def buildAuxTableData[
    MT <: MetaTypes with ({ type DatabaseTableNameImpl = (ResourceName, Stage); type DatabaseColumnNameImpl = ColumnName })
  ](
    tables: Iterator[TableDescriptionLike.Dataset[MT]]
  ): Either[Error, Map[DatabaseTableName[(DatasetInternalName, Stage)], QueryCoordinatorClient.AuxiliaryTableData]] = {
    val result =
      tables.foldLeft(Map.empty[DatabaseTableName[(DatasetInternalName, Stage)], QueryCoordinatorClient.AuxiliaryTableData]) { (acc, dsInfo) =>
        val bits = getRecord(dsInfo.name) match {
          case Right(dsn) => dsn
          case Left(err) => return Left(err)
        }

        val datasetName = DatabaseTableName((bits.internalName, bits.stage))

        type DCN = DatabaseColumnName[ColumnId]

        val locationColumnMap =
          dsInfo.columns.iterator.foldLeft(Map.empty[DCN, Seq[Option[DCN]]]) { case (acc, (colName, colInfo)) =>
            if(colInfo.typ == com.socrata.soql.types.SoQLLocation) {
              val cid = lookupColumnName(dsInfo.name, colName) match {
                case Right(cid) => cid
                case Left(err) => return Left(err)
              }
              val base = colInfo.name.name
              acc + (cid -> Seq("address", "city", "state", "zip").map { suffix =>
                       lookupColumnName(dsInfo.name, DatabaseColumnName(ColumnName(s"${base}_${suffix}"))) match {
                         case Right(cid) => Some(cid)
                         case Left(UnknownColumn(_, _)) => None
                         case Left(err) => return Left(err)
                       }
                     })
            } else {
              acc
            }
          }

        val auxTableData = QueryCoordinatorClient.AuxiliaryTableData(
          locationColumnMap,
          bits.resourceName,
          bits.truthDataVersion
        )

        acc + (datasetName -> auxTableData)
      }

    Right(result)
  }
}

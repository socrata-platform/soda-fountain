package com.socrata.soda.server.util

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName

import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.server.id.{DatasetInternalName, ResourceName, ColumnId}

object DAOCache {
  sealed abstract class Error
  case class UnknownTable(table: DatabaseTableName[(ResourceName, Stage)]) extends Error
  case class UnknownColumn(table: DatabaseTableName[(ResourceName, Stage)], column: DatabaseColumnName[ColumnName]) extends Error
}

class DAOCache(store: NameAndSchemaStore) {
  import DAOCache._

  private case class RelevantBits(
    internalName: DatasetInternalName,
    stage: Stage,
    schema: Map[ColumnName, ColumnId]
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
                record.systemId,
                stage,
                record.columns.iterator.map { col =>
                  col.fieldName -> col.id
                }.toMap
              )
            datasetCache += name -> bits
            Right(bits)
          case None =>
            Left(UnknownTable(dtn))
        }
    }
  }

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

  def buildLocationColumnMap[
    MT <: MetaTypes with ({ type DatabaseTableNameImpl = (ResourceName, Stage); type DatabaseColumnNameImpl = ColumnName }),
    MT2 <: MetaTypes with ({ type DatabaseTableNameImpl = (DatasetInternalName, Stage); type DatabaseColumnNameImpl = ColumnId })
  ](
    tables: Iterator[TableDescriptionLike.Dataset[MT]]
  ): Either[Error, Map[types.DatabaseTableName[MT2], Map[types.DatabaseColumnName[MT2], Seq[Option[types.DatabaseColumnName[MT2]]]]]] = {
    val result =
      tables.foldLeft(Map.empty[types.DatabaseTableName[MT2], Map[types.DatabaseColumnName[MT2], Seq[Option[types.DatabaseColumnName[MT2]]]]]) { (acc, dsInfo) =>
        dsInfo.columns.iterator.foldLeft(acc) { case (acc, (colName, colInfo)) =>
          if(colInfo.typ == com.socrata.soql.types.SoQLLocation) {
            val datasetName = lookupTableName(dsInfo.name) match {
              case Right(dsn) => dsn
              case Left(err) => return Left(err)
            }
            val cid = lookupColumnName(dsInfo.name, colName) match {
              case Right(cid) => cid
              case Left(err) => return Left(err)
            }
            val base = colInfo.name.name
            val dsColumns = acc.getOrElse(datasetName, Map.empty) +
              (cid -> Seq("address", "city", "state", "zip").map { suffix =>
                 lookupColumnName(dsInfo.name, DatabaseColumnName(ColumnName(s"${base}_${suffix}"))) match {
                   case Right(cid) => Some(cid)
                   case Left(UnknownColumn(_, _)) => None
                   case Left(err) => return Left(err)
                 }
               })
            acc + (datasetName -> dsColumns)
          } else {
            acc
          }
        }
      }
    Right(result)
  }
}

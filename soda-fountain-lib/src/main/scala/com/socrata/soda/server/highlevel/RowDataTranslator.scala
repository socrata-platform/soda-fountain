package com.socrata.soda.server.highlevel

import com.rojoma.json.ast._
import com.socrata.soda.server.persistence.{ColumnRecordLike, DatasetRecordLike}
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep, JsonColumnReadRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLValue, SoQLType}
import scala.collection.Map

class RowDataTranslator(dataset: DatasetRecordLike, ignoreUnknownColumns: Boolean) {
  import RowDataTranslator._

  private[this] sealed abstract class ColumnResult
  private[this] case class NoColumn(fieldName: ColumnName) extends ColumnResult
  private[this] case class ColumnInfo(columnRecord: ColumnRecordLike, rRep: JsonColumnReadRep, wRep: JsonColumnWriteRep) extends ColumnResult

  private[this] val legacyDeleteFlag = new ColumnName(":deleted")

  // A cache from the keys of the JSON objects which are rows to values
  // which represent either the fact that the key does not represent
  // a known column or the column's ID and type.
  private[this] val columns = dataset.columnsByName
  private[this] val columnInfos = new java.util.HashMap[String, ColumnResult]
  private[this] def ciFor(rawColumnName: String): ColumnResult = columnInfos.get(rawColumnName) match {
    case null =>
      val cn = ColumnName(rawColumnName)
      columns.get(cn) match {
        case Some(cr) =>
          if(columnInfos.size > columns.size * 10)
            columnInfos.clear() // bad user, but I'd rather spend CPU than memory
        val ci = ColumnInfo(cr, JsonColumnRep.forClientType(cr.typ), JsonColumnRep.forDataCoordinatorType(cr.typ))
          columnInfos.put(rawColumnName, ci)
          ci
        case None =>
          val nc = NoColumn(cn)
          columnInfos.put(rawColumnName, nc)
          nc
      }
    case r =>
      r
  }

  def clientJsonToSoql(row: JValue): RowTranslatorResult = row match {
    case JObject(map) =>
      var rowHasLegacyDeleteFlag = false
      val rowWithSoQLValues: scala.collection.Map[String, SoQLValue] = map.flatMap { case (uKey, uVal) =>
        ciFor(uKey) match {
          case ColumnInfo(cr, rRep, wRep) =>
            rRep.fromJValue(uVal) match {
              case Some(v) => (cr.fieldName.name -> v) :: Nil
              case None => return MaltypedDataError(cr.fieldName, rRep.representedType, uVal)
            }
          case NoColumn(colName) =>
            if(colName == legacyDeleteFlag && JBoolean.canonicalTrue == uVal) {
              rowHasLegacyDeleteFlag = true
              Nil
            } else if(ignoreUnknownColumns) {
              Nil
            } else {
              return UnknownColumnError(colName)
            }
        }
      }
      if(rowHasLegacyDeleteFlag) {
        rowWithSoQLValues.get(dataset.primaryKey.underlying) match {
          case Some(pkVal) => makeDeleteResponse(pkVal)
          case None        => DeleteNoPKError
        }
      } else {
        ValidUpsert(rowWithSoQLValues)
      }
    case JArray(Seq(rowIdJval)) =>
      val pkCol = dataset.columnsById(dataset.primaryKey)
      JsonColumnRep.forClientType(pkCol.typ).fromJValue(rowIdJval) match {
        case Some(soqlVal) => makeDeleteResponse(soqlVal)
        case None => MaltypedDataError(pkCol.fieldName, pkCol.typ, rowIdJval)
      }
    case other =>
      NotAnObjectOrSingleElementArrayError(other)
  }

  private def makeDeleteResponse(pk: SoQLValue): RowTranslatorResult = {
    val pkColumn = dataset.columnsById(dataset.primaryKey)
    val idToDelete = JsonColumnRep.forDataCoordinatorType(pkColumn.typ).toJValue(pk)
    ValidDelete(idToDelete)
  }

  def soqlToDataCoordinatorJson(row: Map[String, SoQLValue]): RowTranslatorResult = {
    val rowWithDCJValues: scala.collection.Map[String, JValue] = row.map { case (uKey, uVal) =>
      ciFor(uKey) match {
        case ColumnInfo(cr, rRep, wRep) => (cr.id.underlying -> wRep.toJValue(uVal))
        case NoColumn(colName) => return UnknownColumnError(colName)
      }
    }

    ValidUpsertForDataCoordinator(rowWithDCJValues)
  }
}

object RowDataTranslator {
  sealed trait RowTranslatorResult
  sealed trait RowTranslatorSuccess extends RowTranslatorResult
  sealed trait RowTranslatorError extends RowTranslatorResult

  case class ValidUpsert(rowData: Map[String, SoQLValue]) extends RowTranslatorSuccess
  case class ValidUpsertForDataCoordinator(rowData: Map[String, JValue]) extends RowTranslatorSuccess
  case class ValidDelete(pk: JValue) extends RowTranslatorSuccess
  case class MaltypedDataError(col: ColumnName, expected: SoQLType, got: JValue) extends RowTranslatorError
  case class UnknownColumnError(col: ColumnName) extends RowTranslatorError
  case object DeleteNoPKError extends RowTranslatorError
  case class NotAnObjectOrSingleElementArrayError(obj: JValue) extends RowTranslatorError
}
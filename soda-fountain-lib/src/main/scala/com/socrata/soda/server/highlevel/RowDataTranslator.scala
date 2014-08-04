package com.socrata.soda.server.highlevel

import com.rojoma.json.ast._
import com.socrata.soda.clients.datacoordinator.{DeleteRow, UpsertRow, RowUpdate}
import com.socrata.soda.server.computation.ComputedColumns
import com.socrata.soda.server.persistence.{DatasetRecordLike, ColumnRecordLike}
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, JsonColumnRep, JsonColumnWriteRep, JsonColumnReadRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLValue, SoQLType}

/**
 * Translates row data from SODA2 client JSON to SoQLValues
 * and from SoQLValues to data coordinator JSON
 * @param dataset Dataset schema information loaded from the database
 * @param ignoreUnknownColumns Indicates whether unrecognized columns
 *                             should cause an error to be thrown or
 *                             simply be ignored.
 */
class RowDataTranslator(dataset: DatasetRecordLike, ignoreUnknownColumns: Boolean) {
  import RowDataTranslator._

  private[this] sealed abstract class ColumnResult
  private[this] case class NoColumn(fieldName: ColumnName) extends ColumnResult
  private[this] case class ColumnInfo(columnRecord: ColumnRecordLike, rRep: JsonColumnReadRep, wRep: JsonColumnWriteRep) extends ColumnResult

  private[this] val legacyDeleteFlag = new ColumnName(":deleted")

  private[this] class ColumnCache[T](map: Map[T, ColumnRecordLike], makeKey: String => T) {
    private val cache = collection.mutable.Map.empty[String, ColumnResult]

    def get(rawKey: String): ColumnResult = cache.getOrElseUpdate(rawKey, addToCache(rawKey))

    private def addToCache(rawKey: String): ColumnResult = {
      val key = makeKey(rawKey)
      map.get(key) match {
        case Some(cr) =>
          if (cache.size > map.size * 10)
            cache.clear() // bad user, but I'd rather spend CPU than memory
          ColumnInfo(cr, JsonColumnRep.forClientType(cr.typ), JsonColumnRep.forDataCoordinatorType(cr.typ))
        case None     => NoColumn(ColumnName(rawKey))
      }
    }
  }

  private[this] val columnNameCache = new ColumnCache(dataset.columnsByName, ColumnName(_))

  def getInfoForColumnList(userColumnList: Seq[String]): Seq[ColumnRecordLike] = {
    userColumnList.map { userColumnName =>
      columnNameCache.get(userColumnName) match {
        case ColumnInfo(ci, _, _) => ci
        case NoColumn(colName)    => throw UnknownColumnEx(colName)
      }
    }
  }

  def transformRowsForUpsert(cc: ComputedColumns[_], rows: Iterator[JValue]): Iterator[RowUpdate] = {
    import ComputedColumns._

    val rowsAsSoql = rows.map(clientJsonToSoql)
    val computedColumns = cc.findComputedColumns(dataset)
    val computedResult = cc.addComputedColumns(rowsAsSoql, computedColumns)
    computedResult match {
      case ComputeSuccess(computedRows) =>
        val rowUpdates = computedRows.map {
          case UpsertAsSoQL(rowData) => UpsertRow(soqlToDataCoordinatorJson(rowData))
          case DeleteAsCJson(pk) => DeleteRow(pk)
        }
        rowUpdates
      case HandlerNotFound(typ) => throw ComputationHandlerNotFoundEx(typ)
    }
  }

  def clientJsonToSoql(row: JValue): Computable = row match {
    case JObject(map) =>
      var rowHasLegacyDeleteFlag = false
      val rowWithSoQLValues = map.flatMap { case (uKey, uVal) =>
        columnNameCache.get(uKey) match {
          case ColumnInfo(cr, rRep, wRep) =>
            if (cr.computationStrategy.isDefined) {
              throw ComputedColumnNotWritableEx(cr.fieldName)
            }
            rRep.fromJValue(uVal) match {
              case Some(v) => (cr.fieldName.name -> v) :: Nil
              case None => throw MaltypedDataEx(cr.fieldName, rRep.representedType, uVal)
            }
          case NoColumn(colName) =>
            if(colName == legacyDeleteFlag && JBoolean.canonicalTrue == uVal) {
              rowHasLegacyDeleteFlag = true
              Nil
            } else if(ignoreUnknownColumns) {
              Nil
            } else {
              throw UnknownColumnEx(colName)
            }
        }
      }
      if(rowHasLegacyDeleteFlag) {
        rowWithSoQLValues.get(dataset.primaryKey.underlying) match {
          case Some(pkVal) => makeDeleteResponse(pkVal)
          case None        => throw DeleteNoPKEx
        }
      } else {
        UpsertAsSoQL(rowWithSoQLValues.toMap)
      }
    case JArray(Seq(rowIdJval)) =>
      val pkCol = dataset.columnsById(dataset.primaryKey)
      JsonColumnRep.forClientType(pkCol.typ).fromJValue(rowIdJval) match {
        case Some(soqlVal) => makeDeleteResponse(soqlVal)
        case None => throw MaltypedDataEx(pkCol.fieldName, pkCol.typ, rowIdJval)
      }
    case other =>
      throw NotAnObjectOrSingleElementArrayEx(other)
  }

  private def makeDeleteResponse(pk: SoQLValue): DeleteAsCJson = {
    val pkColumn = dataset.columnsById(dataset.primaryKey)
    val idToDelete = JsonColumnRep.forDataCoordinatorType(pkColumn.typ).toJValue(pk)
    DeleteAsCJson(idToDelete)
  }

  def soqlToDataCoordinatorJson(row: Map[String, SoQLValue]): Map[String, JValue] = {
    val rowWithDCJValues = row.map { case (uKey, uVal) =>
      columnNameCache.get(uKey) match {
        case ColumnInfo(cr, rRep, wRep) => cr.id.underlying -> wRep.toJValue(uVal)
        case NoColumn(colName)          => throw UnknownColumnEx(colName)
      }
    }

    rowWithDCJValues.toMap
  }
}

object RowDataTranslator {
  sealed trait Computable
  case class UpsertAsSoQL(rowData: Map[String, SoQLValue]) extends Computable
  case class DeleteAsCJson(pk: JValue) extends Computable

  case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: JValue) extends Exception
  case class UnknownColumnEx(col: ColumnName) extends Exception
  case object DeleteNoPKEx extends Exception
  case class NotAnObjectOrSingleElementArrayEx(obj: JValue) extends Exception
  case class ComputedColumnNotWritableEx(column: ColumnName) extends Exception
  case class ComputationHandlerNotFoundEx(typ: ComputationStrategyType.Value) extends Exception
}
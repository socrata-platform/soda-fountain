package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.conversions._
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator.{DeleteRow, RowUpdate, UpsertRow}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.SodaInvalidRequestException
import com.socrata.soda.server.persistence.{ColumnRecordLike, DatasetRecordLike}
import com.socrata.soda.server.wiremodels.{JsonColumnReadRep, JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNull, SoQLType, SoQLValue}

/**
 * Translates row data from SODA2 client JSON to SoQLValues
 * and from SoQLValues to data coordinator JSON
 * @param dataset Dataset schema information loaded from the database
 * @param ignoreUnknownColumns Indicates whether unrecognized columns
 *                             should cause an error to be thrown or
 *                             simply be ignored.
 */
class RowDataTranslator(requestId: RequestId,
                        dataset: DatasetRecordLike,
                        ignoreUnknownColumns: Boolean) {
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
  private[this] val columnIdCache = new ColumnCache(dataset.columnsById, ColumnId(_))

  def getInfoForColumnList(columnIds: Seq[String]): Seq[ColumnRecordLike] = {
    columnIds.map { id =>
      columnIdCache.get(id) match {
        case ColumnInfo(ci, _, _) => ci
        case NoColumn(colName)    => throw UnknownColumnEx(ColumnName(id))
      }
    }
  }

  def transformClientRowsForUpsert(rows: Iterator[JValue]): Iterator[RowUpdate] = {
    val rowsAsSoql = rows.map(clientJsonToComputable)
    transformRowsForUpsert(rowsAsSoql)
  }

  def transformDcRowsForUpsert(schema: ExportDAO.CSchema,
                               rows: Iterator[Array[SoQLValue]]): Iterator[RowUpdate] = {
    val columnIds = schema.schema.map(_.id.underlying)
    val computableRows = rows.map { fields =>
      val fieldMap = for (idx <- 0 to columnIds.length - 1) yield {
        columnIds(idx) -> fields(idx)
      }
      UpsertAsSoQL(fieldMap.toMap)
    }

    transformRowsForUpsert(computableRows)
  }

  private def transformRowsForUpsert(rows: Iterator[Computable]): Iterator[RowUpdate] = {
    rows.map {
      case UpsertAsSoQL(rowData) => UpsertRow(soqlToDataCoordinatorJson(rowData))
      case DeleteAsCJson(pk) => DeleteRow(pk)
    }
  }

  private def clientJsonToComputable(row: JValue): Computable = row match {
    case JObject(map) =>
      var rowHasLegacyDeleteFlag = false
      val rowWithSoQLValues = map.flatMap { case (uKey, uVal) =>
        columnNameCache.get(uKey) match {
          case ColumnInfo(cr, rRep, wRep) =>
            rRep.fromJValue(uVal) match {
              case Some(v) =>
                // allow upsert create by either not specifying :id or specifying :id as null
                if (v == SoQLNull && cr.id.underlying == SystemPrimaryKey) Nil
                else (cr.id.underlying -> v) :: Nil
              case None => throw MaltypedDataEx(cr.fieldName, rRep.representedType, uVal)
            }
          case NoColumn(colName) =>
            if(colName == legacyDeleteFlag) {
              rowHasLegacyDeleteFlag = (JBoolean.canonicalTrue == uVal)
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

  private def soqlToDataCoordinatorJson(row: Map[String, SoQLValue]): Map[String, JValue] = {
    val rowWithDCJValues = row.map { case (uKey, uVal) =>
      columnIdCache.get(uKey) match {
        case ColumnInfo(cr, rRep, wRep) => cr.id.underlying -> wRep.toJValue(uVal)
        case NoColumn(colName)          => throw UnknownColumnEx(colName)
      }
    }

    rowWithDCJValues.toMap
  }
}

object RowDataTranslator {

  val SystemPrimaryKey = ":id"

  sealed trait Computable
  case class UpsertAsSoQL(rowData: Map[String, SoQLValue]) extends Computable
  case class DeleteAsCJson(pk: JValue) extends Computable

  case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: JValue) extends SodaInvalidRequestException(s"Expected $expected type for column $col, but got value $got")
  case class UnknownColumnEx(col: ColumnName) extends SodaInvalidRequestException(s"Unrecognized column $col")
  case object DeleteNoPKEx extends SodaInvalidRequestException(s"Cannot delete row without giving primary key")
  case class NotAnObjectOrSingleElementArrayEx(obj: JValue) extends SodaInvalidRequestException(s"Inappropriate JValue $obj")
}
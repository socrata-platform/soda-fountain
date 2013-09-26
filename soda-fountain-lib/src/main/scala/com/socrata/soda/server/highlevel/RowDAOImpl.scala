package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.persistence.{ColumnRecordLike, DatasetRecordLike, NameAndSchemaStore}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.clients.datacoordinator._
import com.rojoma.json.ast._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import com.socrata.soda.server.wiremodels.{StringColumnRep, JsonColumnReadRep, JsonColumnRep}
import com.socrata.soda.server.highlevel.RowDAO.RowNotAnObject
import com.socrata.soda.clients.datacoordinator.UpsertRow
import scala.Some
import com.socrata.soda.server.highlevel.RowDAO.MaltypedData
import com.socrata.soda.clients.datacoordinator.DeleteRow
import com.socrata.soda.server.highlevel.RowDAO.DatasetNotFound
import com.socrata.soda.server.highlevel.RowDAO.Success
import com.socrata.soda.server.id.RowSpecifier
import com.socrata.soda.server.highlevel.RowDAO.StreamSuccess
import com.socrata.soda.server.highlevel.RowDAO.UnknownColumn
import com.socrata.soda.clients.datacoordinator.RowUpdateOptionChange

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[RowDAOImpl])
  def user = {
    log.info("Actually get user info from somewhere")
    "soda-fountain"
  }

  def query(resourceName: ResourceName, query: String): Result = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val (code, response) = qc.query(datasetRecord.systemId, query, datasetRecord.columnsByName.mapValues(_.id))
        Success(code, response) // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  def getRow(resourceName: ResourceName, rowId: RowSpecifier): Result = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        val stringRep = StringColumnRep.forSoQLLiteral(pkCol.typ)
        stringRep.fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val literal = stringRep.toSoQLLiteral(soqlValue)
            val (code, response) = qc.query(datasetRecord.systemId, s"select * where `${pkCol.fieldName}` = ${literal}", datasetRecord.columnsByName.mapValues(_.id))
            response match {
              case JArray(Seq(row:JObject)) => Success(code, row) // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
              case JArray(Seq()) => RowNotFound(rowId.underlying)
            }
          case None => MaltypedData(pkCol.fieldName, pkCol.typ, JString(rowId.underlying))
        }
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  val LegacyDeleteFlag = new ColumnName(":deleted")

  private case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: JValue) extends Exception
  private case class UnknownColumnEx(col: ColumnName) extends Exception
  private case class DeleteNoPKEx() extends Exception
  private case class NotAnObjectOrSingleElementArrayEx(obj: JValue) extends Exception

  class RowDataTranslator(dataset: DatasetRecordLike, ignoreUnknownColumns: Boolean) {
    private[this] sealed abstract class ColumnResult
    private[this] case class NoColumn(fieldName: ColumnName) extends ColumnResult
    private[this] case class ColumnInfo(columnRecord: ColumnRecordLike, rep: JsonColumnReadRep) extends ColumnResult

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
            val ci = ColumnInfo(cr, JsonColumnRep.forClientType(cr.typ))
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

    def convert(row: JValue): RowUpdate = row match {
      case JObject(map) =>
        var rowHasLegacyDeleteFlag = false
        val row: scala.collection.Map[String, JValue] = map.flatMap { case (uKey, uVal) =>
          ciFor(uKey) match {
            case ColumnInfo(cr, rep) =>
              rep.fromJValue(uVal) match {
                case Some(v) => (cr.id.underlying -> uVal) :: Nil
                case None => throw MaltypedDataEx(cr.fieldName, rep.representedType, uVal)
              }
            case NoColumn(colName) =>
              if(colName == LegacyDeleteFlag && JBoolean.canonicalTrue == uVal) {
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
          row.get(dataset.primaryKey.underlying) match {
            case Some(pkVal) => DeleteRow(pkVal)
            case None => throw DeleteNoPKEx()
          }
        } else {
          UpsertRow(row)
        }
        //TODO: handle a single-element array for deletes!
        /*
      case JArray(Seq(rowIdJval)) =>
        val pkCol = dataset.columnsById(dataset.primaryKey)
        TypeChecker.check(pkCol.typ, rowIdJval) match {
          case Right(idVal) =>
            DeleteRow(TypeChecker.encode(idVal))
          case Left(TypeChecker.Error(expected, got)) =>
            throw MaltypedDataEx(pkCol.fieldName, expected, got)
        }
        */
      case other =>
        throw NotAnObjectOrSingleElementArrayEx(other)
    }
  }

  def doUpsertish[T](resourceName: ResourceName, data: Iterator[JValue], instructions: Iterator[DataCoordinatorInstruction], f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val trans = new RowDataTranslator(datasetRecord, ignoreUnknownColumns = false)
        val upserts = data.map(trans.convert)
        try {
          dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, instructions ++ upserts) {
            case DataCoordinatorClient.Success(result, _) =>
              f(StreamSuccess(result))
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              // hm, if we get schema out of date here, we're pretty much out of luck, since we'll
              // have used up "upserts".  Unless we want to spool it to disk, but for something
              // that SHOULD occur with only low probability that's pretty expensive.
              //
              // I guess we'll refresh our own schema and then toss an error to the user?
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              f(SchemaOutOfSync)
          }
        } catch {
          case UnknownColumnEx(col) =>
            f(UnknownColumn(col))
          case DeleteNoPKEx() =>
            f(DeleteWithoutPrimaryKey)
          case NotAnObjectOrSingleElementArrayEx(v) =>
            f(RowNotAnObject(v))
          case MaltypedDataEx(cn, expected, got) =>
            f(MaltypedData(cn, expected, got))
        }
      case None =>
        f(DatasetNotFound(resourceName))
    }
  }

  def upsert[T](resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(resourceName, data, Iterator.empty, f)

  def replace[T](resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(resourceName, data, Iterator.single(RowUpdateOptionChange(truncate = true)), f)

  def deleteRow[T](resourceName: ResourceName, rowId: RowSpecifier)(f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        StringColumnRep.forSoQLLiteral(pkCol.typ).fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val jvalToDelete = JsonColumnRep.forDataCoordinatorType(pkCol.typ).toJValue(soqlValue)
            doUpsertish(resourceName, Iterator.single(JArray(Seq(jvalToDelete))), Iterator.empty, f)
          case None => f(MaltypedData(pkCol.fieldName, pkCol.typ, JString(rowId.underlying)))
        }
      case None => f(DatasetNotFound(resourceName))
    }
  }
}

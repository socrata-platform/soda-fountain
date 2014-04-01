package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.persistence.{DatasetRecord, ColumnRecordLike, DatasetRecordLike, NameAndSchemaStore}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.clients.datacoordinator._
import com.rojoma.json.ast._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import com.socrata.soda.server.wiremodels._
import com.socrata.soda.clients.datacoordinator.UpsertRow
import scala.Some
import com.socrata.soda.server.highlevel.RowDAO.DatasetNotFound
import com.socrata.soda.server.highlevel.RowDAO.QuerySuccess
import com.rojoma.json.ast.JString
import com.socrata.soda.server.highlevel.RowDAO.RowNotFound
import com.socrata.soda.server.id.RowSpecifier
import com.socrata.soda.server.highlevel.RowDAO.StreamSuccess
import com.socrata.soda.server.highlevel.RowDAO.UnknownColumn
import com.socrata.soda.server.highlevel.RowDAO.RowNotAnObject
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.highlevel.RowDAO.MaltypedData
import com.socrata.soda.clients.datacoordinator.DeleteRow
import com.socrata.soda.clients.datacoordinator.RowUpdateOptionChange
import com.socrata.http.server.util.{NoPrecondition, StrongEntityTag, Precondition}
import java.nio.charset.StandardCharsets
import com.socrata.http.server.util.Precondition
import org.apache.http.HttpStatus
import org.joda.time.format.ISODateTimeFormat

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {

  import RowDAOImpl._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[RowDAOImpl])

  val dateTimeParser = ISODateTimeFormat.dateTimeParser

  def query(resourceName: ResourceName, precondition: Precondition, query: String, rowCount: Option[String], secondaryInstance:Option[String]): Result = {
    store.lookupDataset(resourceName)  match {
      case Some(ds) =>
        getRows(ds, precondition, query, rowCount, secondaryInstance)
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  def getRow(resourceName: ResourceName, precondition: Precondition, rowId: RowSpecifier, secondaryInstance:Option[String]): Result = {
    store.lookupDataset(resourceName) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        val stringRep = StringColumnRep.forType(pkCol.typ)
        stringRep.fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val soqlLiteralRep = SoQLLiteralColumnRep.forType(pkCol.typ)
            val literal = soqlLiteralRep.toSoQLLiteral(soqlValue)
            val query = s"select *, :version where `${pkCol.fieldName}` = $literal"
            getRows(datasetRecord, NoPrecondition, query, None, secondaryInstance) match {
              case QuerySuccess(code, _, truthVersion, truthLastModified, simpleSchema, rows) =>
                val version = ColumnName(":version")
                val versionPos = simpleSchema.schema.indexWhere(_.fieldName == version)
                val deVersionedSchema = simpleSchema.copy(schema = simpleSchema.schema.take(versionPos) ++ simpleSchema.schema.drop(versionPos + 1))
                val rowsStream = rows.toStream
                rowsStream.headOption match {
                  case Some(rowWithVersion) if rowsStream.lengthCompare(1) == 0 =>
                    val etag = StrongEntityTag(rowWithVersion(versionPos).toString.getBytes(StandardCharsets.UTF_8))
                    val row = rowWithVersion.take(versionPos) ++ rowWithVersion.drop(versionPos + 1)
                    precondition.check(Some(etag), sideEffectFree = true) match {
                      case Precondition.Passed =>
                        RowDAO.SingleRowQuerySuccess(code, Seq(etag), truthVersion, truthLastModified, deVersionedSchema, row)
                      case f: Precondition.Failure =>
                        RowDAO.PreconditionFailed(f)
                    }
                  case Some(rowWithVersion) =>
                    TooManyRows
                  case _ =>
                    precondition.check(None, sideEffectFree = true) match {
                      case Precondition.Passed =>
                        RowDAO.RowNotFound(rowId)
                      case f: Precondition.Failure =>
                        RowDAO.PreconditionFailed(f)
                    }
                }
              case other =>
                other
            }
          case None => RowNotFound(rowId) // it's not a valid value and therefore trivially not found
        }
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  private def getRows(ds: DatasetRecord, precondition: Precondition, query: String, rowCount: Option[String], secondaryInstance:Option[String]): Result = {
    import HttpStatus._
    val (code, etags, response) = qc.query(ds.systemId, precondition, query, ds.columnsByName.mapValues(_.id), rowCount, secondaryInstance)
    code match {
      case SC_OK =>
        val cjson = response.asInstanceOf[JArray]
        CJson.decode(cjson.toIterator) match {
          case CJson.Decoded(schema, rows) =>
            schema.pk.map(ds.columnsById(_).fieldName)
            val simpleSchema = ExportDAO.CSchema(
              schema.approximateRowCount,
              schema.dataVersion,
              schema.lastModified.map(time => dateTimeParser.parseDateTime(time)),
              schema.locale,
              schema.pk.map(ds.columnsById(_).fieldName),
              schema.rowCount,
              schema.schema.map { f => ColumnInfo(ColumnName(f.c.underlying), f.c.underlying, f.t) }
            )
            // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
            QuerySuccess(code, etags, ds.truthVersion, ds.lastModified, simpleSchema, rows)
        }
      case code if code >= SC_BAD_REQUEST && code < SC_INTERNAL_SERVER_ERROR =>
        RowDAO.InvalidRequest(code, response)
      case _ =>
        // TODO: other status code from query coordinator
        throw new Exception("TODO")
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
    private[this] case class ColumnInfo(columnRecord: ColumnRecordLike, rRep: JsonColumnReadRep, wRep: JsonColumnWriteRep) extends ColumnResult

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

    def convert(row: JValue): RowUpdate = row match {
      case JObject(map) =>
        var rowHasLegacyDeleteFlag = false
        val row: scala.collection.Map[String, JValue] = map.flatMap { case (uKey, uVal) =>
          ciFor(uKey) match {
            case ColumnInfo(cr, rRep, wRep) =>
              rRep.fromJValue(uVal) match {
                case Some(v) => (cr.id.underlying -> wRep.toJValue(v)) :: Nil
                case None => throw MaltypedDataEx(cr.fieldName, rRep.representedType, uVal)
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
      case JArray(Seq(rowIdJval)) =>
        val pkCol = dataset.columnsById(dataset.primaryKey)
        JsonColumnRep.forClientType(pkCol.typ).fromJValue(rowIdJval) match {
          case Some(soqlVal) =>
            val idToDelete = JsonColumnRep.forDataCoordinatorType(pkCol.typ).toJValue(soqlVal)
            DeleteRow(idToDelete)
          case None => throw MaltypedDataEx( pkCol.fieldName, pkCol.typ, rowIdJval)
        }
      case other =>
        throw NotAnObjectOrSingleElementArrayEx(other)
    }
  }

  def doUpsertish[T](user: String, resourceName: ResourceName, data: Iterator[JValue], instructions: Iterator[DataCoordinatorInstruction], f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val trans = new RowDataTranslator(datasetRecord, ignoreUnknownColumns = false)
        val upserts = data.map(trans.convert)
        try {
          dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, instructions ++ upserts) {
            case DataCoordinatorClient.Success(result, _, newVersion, lastModified) =>
              store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified)
              f(StreamSuccess(result))
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              // hm, if we get schema out of date here, we're pretty much out of luck, since we'll
              // have used up "upserts".  Unless we want to spool it to disk, but for something
              // that SHOULD occur with only low probability that's pretty expensive.
              //
              // I guess we'll refresh our own schema and then toss an error to the user?
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              f(SchemaOutOfSync)
            case DataCoordinatorClient.UpsertUserError(code, data) =>
              f(DataCoordinatorUserErrorCode(code, data))
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

  def upsert[T](user: String, resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(user, resourceName, data, Iterator.empty, f)

  def replace[T](user: String, resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(user, resourceName, data, Iterator.single(RowUpdateOptionChange(truncate = true)), f)

  def deleteRow[T](user: String, resourceName: ResourceName, rowId: RowSpecifier)(f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        StringColumnRep.forType(pkCol.typ).fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val jvalToDelete = JsonColumnRep.forDataCoordinatorType(pkCol.typ).toJValue(soqlValue)
            doUpsertish(user, resourceName, Iterator.single(JArray(Seq(jvalToDelete))), Iterator.empty, f)
          case None => f(MaltypedData(pkCol.fieldName, pkCol.typ, JString(rowId.underlying)))
        }
      case None => f(DatasetNotFound(resourceName))
    }
  }
}

object RowDAOImpl {

  object DataCoordinatorUserErrorCode {
    def apply(code: String, data: Map[String, JValue]): UpsertResult = {
      code match {
        case "update.row.no-such-id" =>
          RowNotFound(RowSpecifier(data("value").asInstanceOf[JString].string))
        case unhandled =>
          throw new Exception(s"TODO: Handle error from data coordinator - $unhandled")
      }
    }
  }
}
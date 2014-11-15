package com.socrata.soda.server.highlevel

import java.nio.charset.StandardCharsets

import com.rojoma.json.ast._
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.{NoPrecondition, Precondition, StrongEntityTag}
import com.socrata.http.server.util.RequestId.{ReqIdHeader, RequestId}
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.id.{ResourceName, RowSpecifier}
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.wiremodels._
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {

  import RowDAOImpl._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[RowDAOImpl])

  val dateTimeParser = ISODateTimeFormat.dateTimeParser

  def query(resourceName: ResourceName, precondition: Precondition, ifModifiedSince: Option[DateTime],
            query: String, rowCount: Option[String], copy: Option[Stage], secondaryInstance:Option[String], noRollup: Boolean,
            requestId: RequestId, resourceScope: ResourceScope): Result = {
    store.lookupDataset(resourceName, copy) match {
      case Some(ds) =>
        getRows(ds, precondition, ifModifiedSince, query, rowCount, copy, secondaryInstance, noRollup, requestId, resourceScope)
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  def getRow(resourceName: ResourceName,
             schemaCheck: Seq[ColumnRecord] => Boolean,
             precondition: Precondition,
             ifModifiedSince: Option[DateTime],
             rowId: RowSpecifier,
             copy: Option[Stage],
             secondaryInstance:Option[String],
             noRollup: Boolean,
             requestId: RequestId,
             resourceScope: ResourceScope): Result = {
    store.lookupDataset(resourceName, copy) match {
      case Some(datasetRecord) =>
        if (schemaCheck(datasetRecord.columns)) {
          val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
          val stringRep = StringColumnRep.forType(pkCol.typ)
          stringRep.fromString(rowId.underlying) match {
            case Some(soqlValue) =>
              val soqlLiteralRep = SoQLLiteralColumnRep.forType(pkCol.typ)
              val literal = soqlLiteralRep.toSoQLLiteral(soqlValue)
              val query = s"select *, :version where `${pkCol.fieldName}` = $literal"
              getRows(datasetRecord, NoPrecondition, ifModifiedSince, query, None, copy, secondaryInstance,
                      noRollup, requestId, resourceScope) match {
                case QuerySuccess(_, truthVersion, truthLastModified, rollup, simpleSchema, rows) =>
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
                          RowDAO.SingleRowQuerySuccess(Seq(etag), truthVersion, truthLastModified, deVersionedSchema, row)
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
        }
        else SchemaInvalidForMimeType
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  private def getRows(ds: DatasetRecord, precondition: Precondition, ifModifiedSince: Option[DateTime],
                      query: String, rowCount: Option[String], copy: Option[Stage], secondaryInstance:Option[String], noRollup: Boolean,
                      requestId: RequestId, resourceScope: ResourceScope): Result = {
    val extraHeaders = Map(ReqIdHeader              -> requestId,
                           SodaUtils.ResourceHeader -> ds.resourceName.name,
                           "X-SODA2-DataVersion"    -> ds.truthVersion.toString,
                           "X-SODA2-LastModified"   -> ds.lastModified.toHttpDate)
    qc.query(ds.systemId, precondition, ifModifiedSince, query, ds.columnsByName.mapValues(_.id), rowCount,
             copy, secondaryInstance, noRollup, extraHeaders, resourceScope) {
      case QueryCoordinatorClient.Success(etags, rollup, response) =>
        val cjson = response
        CJson.decode(cjson) match {
          case CJson.Decoded(schema, rows) =>
            schema.pk.map(ds.columnsById(_).fieldName)
            val simpleSchema = ExportDAO.CSchema(
              schema.approximateRowCount,
              schema.dataVersion,
              schema.lastModified.map(time => dateTimeParser.parseDateTime(time)),
              schema.locale,
              schema.pk.map(ds.columnsById(_).fieldName),
              schema.rowCount,
              schema.schema.map { f => ColumnInfo(f.c, ColumnName(f.c.underlying), f.c.underlying, f.t) }
            )
            // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
            QuerySuccess(etags, ds.truthVersion, ds.lastModified, rollup, simpleSchema, rows)
          case err: CJson.Result =>
            throw new RuntimeException(err.getClass.getName)
        }
      case QueryCoordinatorClient.UserError(code, response) =>
        RowDAO.InvalidRequest(code, response)
      case QueryCoordinatorClient.NotModified(etags) =>
        RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags))
      case QueryCoordinatorClient.PreconditionFailed =>
        RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch)
      case _ =>
        // TODO: other status code from query coordinator
        throw new Exception("TODO")
    }
  }

  def doUpsertish[T](user: String,
                     datasetRecord: DatasetRecordLike,
                     data: Iterator[RowUpdate],
                     instructions: Iterator[DataCoordinatorInstruction],
                     requestId: RequestId,
                     f: UpsertResult => T): T = {
    val extraHeaders = Map(ReqIdHeader -> requestId,
                           SodaUtils.ResourceHeader -> datasetRecord.resourceName.name)
    dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, instructions ++ data, extraHeaders) {
      case DataCoordinatorClient.Success(result, _, copyNumber, newVersion, lastModified) =>
        store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
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
  }

  def upsert[T](user: String, datasetRecord: DatasetRecordLike, data: Iterator[RowUpdate], requestId: RequestId)
               (f: UpsertResult => T): T =
    doUpsertish(user, datasetRecord, data, Iterator.empty, requestId, f)

  def replace[T](user: String, datasetRecord: DatasetRecordLike, data: Iterator[RowUpdate], requestId: RequestId)
                (f: UpsertResult => T): T =
    doUpsertish(user, datasetRecord, data, Iterator.single(RowUpdateOptionChange(truncate = true)),
                requestId, f)

  def deleteRow[T](user: String, resourceName: ResourceName, rowId: RowSpecifier, requestId: RequestId)
                  (f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        StringColumnRep.forType(pkCol.typ).fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val jvalToDelete = JsonColumnRep.forDataCoordinatorType(pkCol.typ).toJValue(soqlValue)
            doUpsertish(user, datasetRecord, Iterator.single(DeleteRow(jvalToDelete)), Iterator.empty,
                        requestId, f)
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
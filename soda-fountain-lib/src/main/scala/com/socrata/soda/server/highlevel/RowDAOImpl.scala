package com.socrata.soda.server.highlevel

import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.{NoPrecondition, Precondition, StrongEntityTag}
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.id.{ResourceName, RowSpecifier}
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.wiremodels._
import com.socrata.soda.external.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.stdlib.Context
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import com.socrata.soda.server.resources.DebugInfo

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[RowDAOImpl])

  val dateTimeParser = ISODateTimeFormat.dateTimeParser

  def query(resourceName: ResourceName,
            precondition: Precondition,
            ifModifiedSince: Option[DateTime],
            query: String,
            context: Context,
            rowCount: Option[String],
            copy: Option[Stage],
            secondaryInstance:Option[String],
            noRollup: Boolean,
            obfuscateId: Boolean,
            fuseColumns: Option[String],
            queryTimeoutSeconds: Option[String],
            debugInfo: DebugInfo,
            resourceScope: ResourceScope): Result = {
    store.lookupDataset(resourceName, copy) match {
      case Some(ds) =>
        getRows(ds, precondition, ifModifiedSince, query, context, rowCount, copy, secondaryInstance, noRollup, obfuscateId,
          fuseColumns, queryTimeoutSeconds, debugInfo, resourceScope)
      case None =>
        log.info("dataset not found {}", resourceName.name)
        DatasetNotFound(resourceName)
    }
  }

  def getRow(resourceName: ResourceName,
             precondition: Precondition,
             ifModifiedSince: Option[DateTime],
             rowId: RowSpecifier,
             copy: Option[Stage],
             secondaryInstance:Option[String],
             noRollup: Boolean,
             obfuscateId: Boolean,
             fuseColumns: Option[String],
             queryTimeoutSeconds: Option[String],
             debugInfo: DebugInfo,
             resourceScope: ResourceScope): Result = {
    store.lookupDataset(resourceName, copy) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        val stringRep = StringColumnRep.forType(pkCol.typ)
        stringRep.fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val soqlLiteralRep = SoQLLiteralColumnRep.forType(pkCol.typ)
            val literal = soqlLiteralRep.toSoQLLiteral(soqlValue)
            val query = s"select *, :version where `${pkCol.fieldName}` = $literal"
            getRows(datasetRecord, NoPrecondition, ifModifiedSince, query,
                    Context.empty, // no context needed
                    None, copy, secondaryInstance,
                    noRollup, obfuscateId, fuseColumns, queryTimeoutSeconds, debugInfo, resourceScope) match {
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
      case None =>
        DatasetNotFound(resourceName)
    }
  }

  private def getRows(ds: DatasetRecord,
                      precondition: Precondition,
                      ifModifiedSince: Option[DateTime],
                      query: String,
                      context: Context,
                      rowCount: Option[String],
                      copy: Option[Stage],
                      secondaryInstance:Option[String],
                      noRollup: Boolean,
                      obfuscateId: Boolean,
                      fuseColumns: Option[String],
                      queryTimeoutSeconds: Option[String],
                      debugInfo: DebugInfo,
                      resourceScope: ResourceScope): Result = {
    val extraHeaders = Map(
                           "X-SODA2-DataVersion"    -> ds.truthVersion.toString,
                           "X-SODA2-LastModified"   -> ds.lastModified.toHttpDate) ++
      fuseColumns.map(c => Map("X-Socrata-Fuse-Columns" -> c)).getOrElse(Map.empty) ++
      (if (debugInfo.debugLogging) Map("X-Socrata-Debug" -> "true") else Map.empty) ++
      (if (debugInfo.explain) Map("X-Socrata-Explain" -> "true") else Map.empty) ++
      (if (debugInfo.analyze) Map("X-Socrata-Analyze" -> "true") else Map.empty)
    qc.query(ds.handle, precondition, ifModifiedSince, query, context, rowCount,
             copy, secondaryInstance, noRollup, obfuscateId, extraHeaders, queryTimeoutSeconds, resourceScope) {
      case QueryCoordinatorClient.Success(etags, rollup, lastModifiedStr, response) if !debugInfo.explain =>
        val jsonColumnReps = if (obfuscateId) JsonColumnRep.forDataCoordinatorType
                             else JsonColumnRep.forDataCoordinatorTypeClearId
        val decodedResult = CJson.decode(response, jsonColumnReps)
        val lastModified = dateTimeParser.parseDateTime(lastModifiedStr)
        val schema = decodedResult.schema
        val simpleSchema = ExportDAO.CSchema(
          schema.approximateRowCount,
          schema.dataVersion,
          Option(lastModified),
          schema.locale,
          schema.pk.map(ds.columnsById(_).fieldName),
          schema.rowCount,
          schema.schema.map { f =>
            ColumnInfo(f.columnId, ColumnName(f.columnId.underlying), f.typ, None /* Do we even get computed-column specs from QC? */)
          }
        )
        QuerySuccess(etags, ds.truthVersion, lastModified, rollup, simpleSchema, decodedResult.rows)
      case QueryCoordinatorClient.Success(_, _, _, response) if debugInfo.explain =>
        // Just forward this along up
        InfoSuccess(200, response)
        // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
      case QueryCoordinatorClient.NotModified(etags) =>
        RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags))
      case QueryCoordinatorClient.PreconditionFailed =>
        RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch)
      case QueryCoordinatorClient.RequestTimedOut(timeout) =>
        RowDAO.RequestTimedOut(timeout)
      case QueryCoordinatorClient.ServiceUnavailable =>
        RowDAO.ServiceUnavailable
      case QueryCoordinatorClient.TooManyRequests =>
        RowDAO.TooManyRequests
      case QueryCoordinatorClient.QueryCoordinatorResult(status, result) =>
        RowDAO.QCError(status, result)
      case QueryCoordinatorClient.InternalServerErrorResult(status, code, tag, data) =>
        RowDAO.InternalServerError(status, QueryCoordinatorClient.client, code, tag, data)
      case x =>
        log.warn("case is NOT implemented %s".format(x.toString))
        RowDAO.InternalServerError(500, QueryCoordinatorClient.client, "unknown", tag, x.toString)
    }
  }

  def doUpsertish[T](user: String,
                     datasetRecord: DatasetRecordLike,
                     expectedDataVersion: Option[Long],
                     data: Iterator[RowUpdate],
                     instructions: Iterator[DataCoordinatorInstruction],
                     f: UpsertResult => T): T = {
    dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user, instructions ++ data) {
      case DataCoordinatorClient.NonCreateScriptResult(result, _, copyNumber, newVersion, newShapeVersion, lastModified) =>
        store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
        f(StreamSuccess(result, newVersion, newShapeVersion))
      case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
        // hm, if we get schema out of date here, we're pretty much out of luck, since we'll
        // have used up "upserts".  Unless we want to spool it to disk, but for something
        // that SHOULD occur with only low probability that's pretty expensive.
        //
        // I guess we'll refresh our own schema and then toss an error to the user?
        store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
        f(SchemaOutOfSync)
      case DataCoordinatorClient.NoSuchRowResult(id, _) =>
        f(RowNotFound(id))
      case DataCoordinatorClient.RowPrimaryKeyNonexistentOrNullResult(id, _) =>
        f(RowPrimaryKeyIsNonexistentOrNull(id))
      case DataCoordinatorClient.DatasetNotFoundResult(_) =>
        f(DatasetNotFound(datasetRecord.resourceName))
      case DataCoordinatorClient.RowNoSuchColumnResult(columnId, _, _) =>
        datasetRecord.columnsById.get(columnId) match {
          case Some(c) => f(UnknownColumn(c.fieldName))
          case None => f(UnknownColumn(new ColumnName("unknown column name")))
        }
      case DataCoordinatorClient.CannotDeleteRowIdResult(_) =>
        f(CannotDeletePrimaryKey)
      case DataCoordinatorClient.UnparsableRowValueResult(_, _, value, _, _ ) =>
        f(RowNotAnObject(value))
      case DataCoordinatorClient.DatasetVersionMismatchResult(_, version) =>
        f(RowDAO.DatasetVersionMismatch(datasetRecord.resourceName, version))
      case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
        f(InternalServerError(500, DataCoordinatorClient.client, code, tag, data))
      case x =>
        log.warn("case is NOT implemented %s".format(x.toString))
        f(InternalServerError(500, DataCoordinatorClient.client,"unknown", tag, x.toString))
    }
  }

  def upsert[T](user: String, datasetRecord: DatasetRecordLike, expectedDataVersion: Option[Long], data: Iterator[RowUpdate], rowUpdateOption: RowUpdateOption)
               (f: UpsertResult => T): T =
    doUpsertish(user, datasetRecord, expectedDataVersion, data, Iterator.single(rowUpdateOption), f)

  def replace[T](user: String, datasetRecord: DatasetRecordLike, expectedDataVersion: Option[Long], data: Iterator[RowUpdate])
                (f: UpsertResult => T): T =
    doUpsertish(user, datasetRecord, expectedDataVersion, data, Iterator.single(RowUpdateOption.default.copy(truncate = true)),
                f)

  def deleteRow[T](user: String, resourceName: ResourceName, expectedDataVersion: Option[Long], rowId: RowSpecifier)
                  (f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some(datasetRecord) =>
        val pkCol = datasetRecord.columnsById(datasetRecord.primaryKey)
        StringColumnRep.forType(pkCol.typ).fromString(rowId.underlying) match {
          case Some(soqlValue) =>
            val jvalToDelete = JsonColumnRep.forDataCoordinatorType(pkCol.typ).toJValue(soqlValue)
            doUpsertish(user, datasetRecord, expectedDataVersion, Iterator.single(DeleteRow(jvalToDelete)), Iterator.empty,
                        f)
          case None => f(MaltypedData(pkCol.fieldName, pkCol.typ, JString(rowId.underlying)))
        }
      case None => f(DatasetNotFound(resourceName))
    }
  }

  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

}

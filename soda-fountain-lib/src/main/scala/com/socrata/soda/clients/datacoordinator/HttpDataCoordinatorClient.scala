package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.io._
import com.rojoma.json.v3.util.JsonArrayIterator
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.HttpMethods
import com.socrata.http.server.util._
import com.socrata.soda.clients.datacoordinator
import com.socrata.soda.server.id.{ColumnId, RowSpecifier, DatasetId, SecondaryId}
import com.socrata.soda.server.util.schema.SchemaSpec
import javax.servlet.http.HttpServletResponse
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat


abstract class HttpDataCoordinatorClient(httpClient: HttpClient) extends DataCoordinatorClient {
  import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DataCoordinatorClient])

  val dateTimeParser = ISODateTimeFormat.dateTimeParser
  val xhDataVersion = "X-SODA2-Truth-Version"
  val xhLastModified = "X-SODA2-Truth-Last-Modified"
  val xhCopyNumber = "X-SODA2-Truth-Copy-Number"

  def hostO(instance: String): Option[RequestBuilder]
  def createUrl(host: RequestBuilder) = host.p("dataset")
  def mutateUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying)
  def schemaUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying, "schema")
  def secondaryUrl(host: RequestBuilder, secondaryId: SecondaryId, datasetId: DatasetId) = host.p("secondary-manifest", secondaryId.underlying, datasetId.underlying)
  def exportUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying)

  def withHost[T](instance: String)(f: RequestBuilder => T): T =
    hostO(instance) match {
      case Some(host) => f(host)
      case None => throw new Exception(s"could not find data coordinator for instance ${instance}")
    }

  def withHost[T](datasetId: DatasetId)(f: RequestBuilder => T): T =
    withHost(datasetId.nativeDataCoordinator)(f)

  def propagateToSecondary(datasetId: DatasetId,
                           secondaryId: SecondaryId,
                           extraHeaders: Map[String, String] = Map.empty): Unit =
    withHost(datasetId) { host =>
      val r = secondaryUrl(host, secondaryId, datasetId).addHeaders(extraHeaders).jsonBody(JNull)
      httpClient.execute(r).run { response =>
        response.resultCode match {
          case HttpServletResponse.SC_OK => // ok
          case _ => throw new Exception(s"could not propagate to secondary ${secondaryId}")
        }
      }
    }

  implicit class Augmenting(r: RequestBuilder) {
    def precondition(p: Precondition): RequestBuilder = r.addHeaders(PreconditionRenderer(p))
  }

  def headerExists(header: String, resp: Response) = resp.headerNames.contains(header.toLowerCase)

  def getHeader(header: String, resp: Response) = resp.headers(header)(0)

  def getSchema(datasetId: DatasetId): Option[SchemaSpec] = {
    withHost(datasetId) { host =>
      val request = schemaUrl(host, datasetId).get
      httpClient.execute(request).run { response =>
        response.resultCode match {
          case HttpServletResponse.SC_OK => {
            response.value[SchemaSpec]() match {
              case Right(jv) => Some(jv)
              case Left(_) => throw new Exception("Unable to interpret data coordinator's response for " + datasetId + " as a schemaspec?")
            }
          }
          case HttpServletResponse.SC_NOT_FOUND => None
          case _ => throw new Exception("Unexpected result from server: " + response.resultCode)
        }
      }
    }
  }

  def errorFrom[T](r: Response): Option[PossiblyUnknownDataCoordinatorError] =
    r.resultCode match {
      case HttpServletResponse.SC_OK =>
        None
        // unclear why this is a special case.
      case HttpServletResponse.SC_NOT_MODIFIED =>
        Some(NotModified())
      case code =>
        Some(r.value[PossiblyUnknownDataCoordinatorError]().right.toOption.getOrElse(
          throw new Exception(s"Response was JSON but not decodable as an error - code $code")))
    }

  def expectStartOfArray(in: Iterator[JsonEvent]) {
    if(!in.hasNext) throw new JsonParserEOF(Position.Invalid)
    val t = in.next()
    if(!t.isInstanceOf[StartOfArrayEvent]) throw new JsonBadParse(t)
  }

  // TODO: This skip-next-datum stuff should be in a library.
  // Specifically it should be in rojoma-json.
  def skipNextDatum(in: BufferedIterator[JsonEvent]) {
    def skipArray(in: BufferedIterator[JsonEvent]) {
      while(!in.head.isInstanceOf[EndOfArrayEvent]) skipNextDatum(in)
      in.next()
    }

    def skipObject(in: BufferedIterator[JsonEvent]) {
      while(!in.head.isInstanceOf[EndOfObjectEvent]) skipNextDatum(in)
      in.next()
    }

    in.next() match {
      case StartOfArrayEvent() => skipArray(in)
      case StartOfObjectEvent() => skipObject(in)
      case _ => // nothing
    }
  }

  def arrayOfResults(in: BufferedIterator[JsonEvent], alreadyInArray: Boolean = false): Iterator[ReportItem] = {
    if(!alreadyInArray) expectStartOfArray(in)
    new Iterator[ReportItem] {
      var pendingIterator: Iterator[JValue] = null
      def invalidatePending() {
        if((pendingIterator ne null) && pendingIterator.hasNext) {
          throw new Exception("Row result was not completely consumed")
        }
        pendingIterator = null
      }
      def hasNext = { invalidatePending(); in.hasNext && !in.head.isInstanceOf[EndOfArrayEvent] }
      def next(): ReportItem = {
        if(!hasNext) Iterator.empty.next()
        if(in.head.isInstanceOf[StartOfArrayEvent]) {
          pendingIterator = JsonArrayIterator[JValue](in)
          UpsertReportItem(pendingIterator)
        } else {
          skipNextDatum(in)
          OtherReportItem
        }
      }
    }
  }

  /**
   * For cases where we will be running mutations in D.C. Creates an http-post request if method has not yet been defined.
   * @param rb
   * @param script
   * @param f
   * @tparam T
   * @return
   */
  protected def sendScript[T](rb: RequestBuilder, script: MutationScript)(f: Either[Result, Response] => T): T = {
    val request = rb.json(script.it)
    httpClient.execute(request).run { r =>
      errorFrom(r) match {
        case None =>
          if (headerExists(xhDataVersion, r) && headerExists(xhLastModified, r))
            f(Right(r))
          else {
            log.error("No version headers set from data coordinator")
            throw new Exception("No version headers set from data coordinator")
          }
        case Some(err) =>
          err match {
            case(reqError: DCRequestError) => reqError match {
                case ContentTypeBadRequest(contentErrorType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentErrorType)))
                case PreconditionFailed() =>
                  f(Left(PreconditionFailedResult))
                case ContentTypeMissing() =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, "")))
                case ContentTypeUnparsable(contentType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentType)))
                case ContentTypeNotJson(contentType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentType)))
                case ContentTypeUnknownCharset(contentType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentType)))
                case SchemaMismatchForExport(dataset, schema) =>
                  f(Left(SchemaOutOfDateResult(schema)))
                case RequestEntityTooLarge(bytes) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, bytes.toString)))
                case BodyMalformedJson(row, column) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, s"row $row; column $column")))
                case BodyNotJsonArray() =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, "")))
                case SchemaMismatch(dataset, schema, commandIndex) =>
                  f(Left(SchemaOutOfDateResult(schema)))
                case EmptyCommandStream(commandIndex) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, s"commandIndex: $commandIndex" )))
                case CommandIsNotAnObject(value, commandIndex) =>
                  f(Left(InternalServerErrorResult( reqError.code, tag, s"value: $value, commandIndex: $commandIndex")))
                case MissingCommandField(obj, field, commandIndex) =>
                  f(Left(InternalServerErrorResult( reqError.code, tag, s"obj: $obj, field: $field, commandIndex: $commandIndex" )))
                case InvalidCommandFieldValue(obj, field, value, commandIndex) =>
                  f(Left(InternalServerErrorResult( reqError.code, tag, s"obj: $obj, field: $field, value: $value, commandIndex: $commandIndex")))
            }
            case (rowError: DCRowUpdateError) => rowError match {
              case RowPrimaryKeyNonexistentOrNull(dataset, commandIndex) =>
                f(Left(RowPrimaryKeyNonexistentOrNullResult(RowSpecifier(""), commandIndex)))
              case NoSuchRow(_, id, commandIndex) =>
                f(Left(NoSuchRowResult(id, commandIndex)))
              case UnparsableRowValue(_, column, tp, value, commandIndex, commandSubIndex) =>
                f(Left(UnparsableRowValueResult(column, tp, value, commandIndex, commandSubIndex)))
              case RowNoSuchColumn(dataset, column, commandIndex, commandSubIndex) =>
                f(Left(RowNoSuchColumnResult(column, commandIndex, commandSubIndex)))
            }
            case (columnError: DCColumnUpdateError) => columnError match {
              case ColumnAlreadyExists(dataset, column, commandIndex) =>
                f(Left(ColumnExistsAlreadyResult(dataset, column, commandIndex)))
              case IllegalColumnId(id, commandIndex) =>
                f(Left(IllegalColumnIdResult(ColumnId(id), commandIndex)))
              case InvalidSystemColumnOperation(dataset, column, commandIndex) =>
                f(Left(InvalidSystemColumnOperationResult(dataset, column, commandIndex)))
              case ColumnNotFound(dataset, column, commandIndex) =>
                f(Left(ColumnNotFoundResult(dataset, column, commandIndex)))
            }
            case (datasetError: DCDatasetUpdateError) => datasetError match {
              case NoSuchDataset(dataset) =>
                f(Left(DatasetNotFoundResult(dataset)))
              case CannotAcquireDatasetWriteLock(dataset) =>
                f(Left(CannotAcquireDatasetWriteLockResult(dataset)))
              case IncorrectLifecycleStage(dataset, actualStage, expectedStage) =>
                f(Left(IncorrectLifecycleStageResult(actualStage, expectedStage)))
              case InitialCopyDrop(dataset, commandIndex) =>
                f(Left(InitialCopyDropResult(dataset, commandIndex)))
              case OperationAfterDrop(dataset, commandIndex) =>
                f(Left(OperationAfterDropResult(dataset, commandIndex)))
            }
            case (updateError: DCUpdateError) => updateError match {
              case SystemInReadOnlyMode(commandIndex) =>
                f(Left(InternalServerErrorResult(updateError.code, tag, s"commandIndex: $commandIndex")))
              case NoSuchType(tp, commandIndex) =>
                f(Left(NoSuchTypeResult(tp, commandIndex)))
              case RowVersionMismatch(dataset, value, commandIndex, expected, actual) =>
                f(Left(RowVersionMismatchResult(dataset, value, commandIndex, expected, actual)))
              case VersionOnNewRow(dataset, commandIndex) =>
                f(Left(VersionOnNewRowResult(dataset, commandIndex)))
              case ScriptRowDataInvalidValue(dataset, value, commandIndex, commandSubIndex) =>
                f(Left(ScriptRowDataInvalidValueResult(dataset, value, commandIndex, commandSubIndex)))
              case PrimaryKeyAlreadyExists(dataset, column, existingColumn, commandIndex) =>
                f(Left(PrimaryKeyAlreadyExistsResult(dataset, column, existingColumn, commandIndex)))
              case InvalidTypeForPrimaryKey(dataset, column, tp, commandIndex) =>
                f(Left(InvalidTypeForPrimaryKeyResult(dataset, column, tp, commandIndex)))
              case DuplicateValuesInColumn(dataset, column, commandIndex) =>
                f(Left(DuplicateValuesInColumnResult(dataset, column, commandIndex)))
              case NullsInColumn(dataset, column, commandIndex) =>
                f(Left(NullsInColumnResult(dataset, column, commandIndex)))
              case NotPrimaryKey(dataset, column, commandIndex) =>
                f(Left(NotPrimaryKeyResult(dataset, column, commandIndex)))
              case DeleteOnRowId(dataset, column, commandIndex) =>
                f(Left(CannotDeleteRowIdResult(commandIndex)))  // commandIndex available
            }
            case (dcError: DataCoordinatorError) => dcError match {
              case ThreadsMutationError() =>
                f(Left(InternalServerErrorResult(dcError.code, tag, "")))
              case InvalidLocale(locale, commandIndex) =>
                f(Left(InvalidLocaleResult(locale, commandIndex)))
              case NoSuchRollup(name, commandIndex) =>
                f(Left(NoSuchRollupResult(name, commandIndex)))
              case NotModified() =>
                f(Left(NotModifiedResult(etagsSeq(r))))
              case InvalidRowId() =>
                f(Left(InvalidRowIdResult))
            }
            case UnknownDataCoordinatorError(code, data) =>
              log.error("Unknown data coordinator error: code %s, Aux info: %s".format(code, data))
              f(Left(InternalServerErrorResult(code, tag, data.mkString(","))))
          }
      }
    }
  }


  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

  def sendNonCreateScript[T](rb: RequestBuilder, script: MutationScript)(f: Result => T): T =
    sendScript(rb, script) {
      case Right(r) =>
        f(NonCreateScriptResult(
            arrayOfResults(r.jsonEvents().buffered),
            None,
            getHeader(xhCopyNumber, r).toLong,
            getHeader(xhDataVersion, r).toLong,
            dateTimeParser.parseDateTime(getHeader(xhLastModified, r))))
      case Left(e) => f(e)
    }

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US",
             extraHeaders: Map[String, String] = Map.empty): (ReportMetaData, Iterable[ReportItem]) = {
    withHost(instance) { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      sendScript(createUrl(host).addHeaders(extraHeaders), createScript) {
        case Right(r) =>
          val events = r.jsonEvents().buffered
          expectStartOfArray(events)
          if (!events.hasNext || !events.head.isInstanceOf[StringEvent])
            throw new Exception("Bad response from data coordinator: expected dataset id")
          val StringEvent(datasetId) = events.next()
          (ReportMetaData(DatasetId(datasetId),
                          getHeader(xhDataVersion, r).toLong,
                          dateTimeParser.parseDateTime(getHeader(xhLastModified, r))),
           arrayOfResults(events, alreadyInArray = true).toSeq)
        case other =>
          throw new Exception("Unexpected response from data-coordinator: " + other)
      }
    }
  }

  def update[T](datasetId: DatasetId,
                schemaHash: String,
                user: String,
                instructions: Iterator[DataCoordinatorInstruction],
                extraHeaders: Map[String, String] = Map.empty)
               (f: Result => T): T = {
    // TODO: update should decode the row op report into something higher-level than JValues
    withHost(datasetId) { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId).addHeaders(extraHeaders), updateScript)(f)
    }
  }

  def copy[T](datasetId: DatasetId,
              schemaHash: String,
              copyData: Boolean,
              user: String,
              instructions: Iterator[DataCoordinatorInstruction],
              extraHeaders: Map[String, String] = Map.empty)
             (f: Result => T): T = {
    // TODO: copy should decode the row op report into something higher-level than JValues
    withHost(datasetId) { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId).addHeaders(extraHeaders), createScript)(f)
    }
  }

  def publish[T](datasetId: DatasetId,
                 schemaHash: String,
                 snapshotLimit:Option[Int],
                 user: String,
                 instructions: Iterator[DataCoordinatorInstruction],
                 extraHeaders: Map[String, String] = Map.empty)
                (f: Result => T): T = {
    // TODO: publish should decode the row op report into something higher-level than JValues
    withHost(datasetId) { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId).addHeaders(extraHeaders), pubScript)(f)
    }
  }

  def dropCopy[T](datasetId: DatasetId,
                  schemaHash: String,
                  user: String,
                  instructions: Iterator[DataCoordinatorInstruction],
                  extraHeaders: Map[String, String] = Map.empty)
                 (f: Result => T): T = {
    // TODO: dropCopy should decode the row op report into something higher-level than JValues
    withHost(datasetId) { host =>
      val dropScript = new MutationScript(user, DropDataset(schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId).addHeaders(extraHeaders), dropScript)(f)
    }
  }

  // Pretty sure this is completely wrong
  def deleteAllCopies[T](datasetId: DatasetId,
                         schemaHash: String,
                         user: String,
                         extraHeaders: Map[String, String] = Map.empty)
                        (f: Result => T): T = {
    // TODO: deleteAllCopies should decode the row op report into something higher-level than JValues
    withHost(datasetId) { host =>
      val deleteScript = new MutationScript(user, DropDataset(schemaHash), Iterator.empty)
      val req = mutateUrl(host, datasetId).method(HttpMethods.DELETE).addHeaders(extraHeaders)
      sendNonCreateScript(req, deleteScript)(f)
    }
  }

  def checkVersionInSecondary(datasetId: DatasetId,
                              secondaryId: SecondaryId,
                              extraHeaders: Map[String, String] = Map.empty): VersionReport = {
    withHost(datasetId) { host =>
      val request = secondaryUrl(host, secondaryId, datasetId)
        .addHeader(("Content-type", "application/json"))
        .addHeaders(extraHeaders)
        .get
      httpClient.execute(request).run { response =>
        response.resultCode match {
          case HttpServletResponse.SC_OK =>
            val oVer = response.value[VersionReport]()
            oVer match {
              case Right(ver) => ver
              case Left(_) => throw new Exception("version not found")
            }
          case HttpServletResponse. SC_NOT_FOUND => throw new Exception("version not found")
          case _ => throw new Exception("unexpected error code, version not found")
        }
      }
    }
  }

  def export[T](datasetId: DatasetId,
                schemaHash: String,
                columns: Seq[String],
                precondition: Precondition,
                ifModifiedSince: Option[DateTime],
                limit: Option[Long],
                offset: Option[Long],
                copy: String,
                sorted: Boolean,
                rowId: Option[String],
                extraHeaders: Map[String, String] = Map.empty)
               (f: Result => T): T = {
    withHost(datasetId) { host =>
      val limParam = limit.map { limLong => "limit" -> limLong.toString }
      val offParam = offset.map { offLong => "offset" -> offLong.toString }
      val columnsParam = if (columns.isEmpty) None else Some("c" -> columns.mkString(","))
      val rowIdParam = if (rowId.isEmpty) None else rowId.map("row_id" -> _)
      val sortedParam = "sorted" -> sorted.toString
      val request = exportUrl(host, datasetId)
                    .q("schemaHash" -> schemaHash)
                    .addParameter("copy"->copy)
                    .addParameters(limParam ++ offParam ++ columnsParam ++ rowIdParam)
                    .addParameter(sortedParam)
                    .addHeaders(PreconditionRenderer(precondition) ++ ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate))
                    .addHeaders(extraHeaders)
                    .get
      httpClient.execute(request).run { r =>
        errorFrom(r) match {
          case None =>
            f(ExportResult(r.array[JValue](), r.headers("ETag").headOption.map(EntityTagParser.parse(_))))
          case Some(err) =>
            err match {
              case SchemaMismatchForExport(_, newSchema) =>
                f(SchemaOutOfDateResult(newSchema))
              case NotModified() =>
                f(NotModifiedResult(etagsSeq(r)))
              case PreconditionFailed() =>
                f(PreconditionFailedResult)
              case NoSuchDataset(dataset) =>
                f(DatasetNotFoundResult(dataset))
              case cbr: ContentTypeBadRequest =>
                f(InternalServerErrorResult(cbr.code, tag, cbr.contentTypeError))
              case InvalidRowId() =>
                f(InvalidRowIdResult)
              case x: DataCoordinatorError =>
                f(InternalServerErrorResult(x.code, tag, ""))
              case UnknownDataCoordinatorError(code, data) =>
                log.error("Unknown data coordinator error: code %s, Aux info: %s".format(code, data))
                f(InternalServerErrorResult(code, tag, data.mkString(",")))
              case x =>
                log.warn("case is NOT implemented %s".format(x.toString))
                f(InternalServerErrorResult("unknown", tag, x.toString))
            }
        }
      }
    }
  }

  /**
   * EntityTag Seq from response object
   * @param r
   * @return
   */
  def etagsSeq(r: Response): Seq[EntityTag] = {
    r.headers("etag").map(EntityTagParser.parse(_ : String))
  }
}

package com.socrata.soda.clients.datacoordinator

import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import com.socrata.soda.server.id.{DatasetId, SecondaryId}
import com.socrata.http.server.routing.HttpMethods
import com.rojoma.json.ast.JValue
import com.socrata.soda.server.util.schema.SchemaSpec
import javax.servlet.http.HttpServletResponse
import com.socrata.http.server.util._
import com.socrata.soda.clients.datacoordinator
import com.rojoma.json.io._
import com.rojoma.json.util.JsonArrayIterator
import com.rojoma.json.io.StartOfArrayEvent
import com.rojoma.json.io.EndOfArrayEvent
import org.joda.time.format.ISODateTimeFormat
import com.socrata.soda.server.highlevel.ColumnDAO.InvalidRowIdOperation

abstract class HttpDataCoordinatorClient(httpClient: HttpClient) extends DataCoordinatorClient {
  import DataCoordinatorClient._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DataCoordinatorClient])

  val dateTimeParser = ISODateTimeFormat.dateTimeParser
  val xhDataVersion = "X-SODA2-Truth-Version"
  val xhLastModified = "X-SODA2-Truth-Last-Modified"

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

  def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId): Unit =
    withHost(datasetId) { host =>
      val r = secondaryUrl(host, secondaryId, datasetId).method(HttpMethods.POST).get // ick
      for (response <- httpClient.execute(r)) yield {
        response.resultCode match {
          case 200 => // ok
          case _ => throw new Exception(s"could not propagate to secondary ${secondaryId}")
        }
      }
    }

  implicit class Augmenting(r: RequestBuilder) {
    def precondition(p: Precondition): RequestBuilder = r.addHeaders(PreconditionRenderer(p))
  }

  def headerExists(header: String, resp: Response) =
    resp.headerNames.contains(header.toLowerCase)

  def getHeader(header: String, resp: Response) =
    resp.headers(header)(0)

  def getSchema(datasetId: DatasetId): Option[SchemaSpec] =
    withHost(datasetId) { host =>
      val request = schemaUrl(host, datasetId).get
      for (response <- httpClient.execute(request)) yield {
        if(response.resultCode == 200) {
          val result = response.asValue[SchemaSpec]()
          if(!result.isDefined) throw new Exception("Unable to interpret data coordinator's response for " + datasetId + " as a schemaspec?")
          result
        } else if(response.resultCode == 404) {
          None
        } else {
          throw new Exception("Unexpected result from server: " + response.resultCode)
        }
      }
    }

  // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
  // TODO                                                                  TODO
  // TODO :: ALL THESE NEED TO HANDLE ERRORS FROM THE DATA COORDINATOR! :: TODO
  // TODO                                                                  TODO
  // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

  def errorFrom[T](r: Response): Option[PossiblyUnknownDataCoordinatorError] =
    r.resultCode match {
      case HttpServletResponse.SC_OK =>
        None
      case HttpServletResponse.SC_NOT_MODIFIED =>
        Some(datacoordinator.NotModified())
      case code if code >= 400 && code <= 499 =>
        r.asValue[UserErrorReportedByDataCoordinatorError]() match {
          case dcErr@Some(_) =>
            dcErr
          case None =>
            throw new Exception("Response was JSON but not decodable as user error reported by data coordinator")
        }
      case _ =>
        Some(r.asValue[PossiblyUnknownDataCoordinatorError]().getOrElse(throw new Exception("Response was JSON but not decodable as an error")))
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

  protected def sendScript[T](rb: RequestBuilder, script: MutationScript)(f: Either[Result, Response] => T): T = {
    val request = rb.json(script.it)
    for (r <- httpClient.execute(request)) yield {
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
            case SchemaMismatch(_, schema) =>
              f(Left(SchemaOutOfDate(schema)))
            case UserErrorReportedByDataCoordinatorError(code, data) =>
              f(Left(UpsertUserError(code, data)))
            case DeleteOnRowId() =>
              f(Left(CannotDeleteRowId))
            case UnknownDataCoordinatorError(code, data) =>
              log.error("Unknown data coordinator error " + code)
              log.error("Aux info: " + data)
              throw new Exception("Unknown data coordinator error " + code)
          }
      }
    }
  }

  def sendNonCreateScript[T](rb: RequestBuilder, script: MutationScript)(f: Result => T): T =
    sendScript(rb, script) {
      case Right(r) =>
        f(Success(
            arrayOfResults(r.asJsonEvents().buffered),
            None,
            getHeader(xhDataVersion, r).toLong,
            dateTimeParser.parseDateTime(getHeader(xhLastModified, r))))
      case Left(e) => f(e)
    }

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (ReportMetaData, Iterable[ReportItem]) = {
    withHost(instance) { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      sendScript(createUrl(host), createScript) {
        case Right(r) =>
          val events = r.asJsonEvents().buffered
          expectStartOfArray(events)
          if(!events.hasNext || !events.head.isInstanceOf[StringEvent]) throw new Exception("Bad response from data coordinator: expected dataset id")
          val StringEvent(datasetId) = events.next()
          (ReportMetaData(DatasetId(datasetId), getHeader(xhDataVersion, r).toLong, dateTimeParser.parseDateTime(getHeader(xhLastModified, r))),
           arrayOfResults(events, alreadyInArray = true).toSeq)
        case other =>
          throw new Exception("Unexpected response from data-coordinator: " + other)
      }
    }
  }

  def update[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: update should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId), updateScript)(f)
    }
  }

  def copy[T](datasetId: DatasetId, schemaHash: String, copyData: Boolean, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: copy should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId), createScript)(f)
    }
  }
  def publish[T](datasetId: DatasetId, schemaHash: String, snapshotLimit:Option[Int], user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: publish should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId), pubScript)(f)
    }
  }
  def dropCopy[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: dropCopy should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val dropScript = new MutationScript(user, DropDataset(schemaHash), instructions)
      sendNonCreateScript(mutateUrl(host, datasetId), dropScript)(f)
    }
  }

  // Pretty sure this is completely wrong
  def deleteAllCopies[T](datasetId: DatasetId, schemaHash: String, user: String)(f: Result => T): T = {
    log.info("TODO: deleteAllCopies should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val deleteScript = new MutationScript(user, DropDataset(schemaHash), Iterator.empty)
      sendNonCreateScript(mutateUrl(host, datasetId).method(HttpMethods.DELETE), deleteScript)(f)
    }
  }

  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): VersionReport = {
    withHost(datasetId) { host =>
      val request = secondaryUrl(host, secondaryId, datasetId)
        .addHeader(("Content-type", "application/json"))
        .get
      httpClient.execute(request).flatMap{ response =>
        log.info("TODO: Handle errors from the data-coordinator")
        val oVer = response.asValue[VersionReport]()
        oVer match {
          case Some(ver) => ver
          case None => throw new Exception("version not found")
        }
      }
    }
  }

  def export[T](datasetId: DatasetId, schemaHash: String, precondition: Precondition, limit: Option[Long], offset: Option[Long], copy: String, sorted: Boolean)(f: Result => T): T = {
    withHost(datasetId) { host =>
      val limParam = limit.map { limLong => "limit" -> limLong.toString }
      val offParam = offset.map { offLong => "offset" -> offLong.toString }
      val sortedParam = "sorted" -> sorted.toString
      val request = exportUrl(host, datasetId).q("schemaHash" -> schemaHash).addParameter("copy"->copy).addParameters(limParam ++ offParam).addParameter(sortedParam).precondition(precondition).get
      for(r <- httpClient.execute(request)) yield {
        errorFrom(r) match {
          case None =>
            f(Export(r.asArray[JValue](), r.headers("ETag").headOption.map(EntityTagParser.parse(_))))
          case Some(err) =>
            err match {
              case SchemaMismatchForExport(_, newSchema) =>
                f(SchemaOutOfDate(newSchema))
              case datacoordinator.NotModified() =>
                f(NotModified(r.headers("etag").map(EntityTagParser.parse(_ : String))))
              case datacoordinator.PreconditionFailed() =>
                f(DataCoordinatorClient.PreconditionFailed)
            }
        }
      }
    }
  }
}

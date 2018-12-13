package com.socrata.soda.server.resources

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.ReportItem

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.Try
import com.rojoma.json.v3.ast.{JArray, JNumber, JString, JValue}
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.common.util.ContentNegotiation
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.http.server.util.{EntityTag, Precondition, RequestId}
import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, RowUpdate, RowUpdateOption}
import com.socrata.soda.clients.querycoordinator.{QueryCoordinatorClient, QueryCoordinatorError}
import com.socrata.soda.server.{responses => SodaErrors, _}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.export.Exporter
import com.socrata.soda.server.highlevel.{DatasetDAO, RowDAO, RowDataTranslator}
import com.socrata.soda.server.id.{ResourceName, RowSpecifier, SecondaryId}
import com.socrata.soda.server.metrics.{MetricProvider, NoopMetricProvider}
import com.socrata.soda.server.metrics.Metrics.{QuerySuccess => QuerySuccessMetric, _}
import com.socrata.soda.server.persistence.DatasetRecordLike
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.wiremodels.InputUtils
import com.socrata.thirdparty.metrics.Metrics
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * Resource: services for upserting, deleting, and querying dataset rows.
 */
case class Resource(rowDAO: RowDAO,
                    datasetDAO: DatasetDAO,
                    etagObfuscator: ETagObfuscator,
                    maxRowSize: Long,
                    metricProvider: MetricProvider,
                    export: Export,
                    dc: DataCoordinatorClient) extends Metrics {
  import Resource._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  // Unlike the global dispatch latency, these are for measuring only successful Query requests
  val queryLatencyNonRollup = metrics.histogram("query-latency-ms-nonrollup")
  val queryLatencyRollup = metrics.histogram("query-latency-ms-rollup")

  val headerHashAlg = "SHA1"
  val headerHashLength = MessageDigest.getInstance(headerHashAlg).getDigestLength
  val domainIdHeader = "X-SODA2-Domain-Id"

  def headerHash(req: HttpServletRequest) = {
    val hash = MessageDigest.getInstance(headerHashAlg)
    hash.update(Option(req.getQueryString).toString.getBytes(StandardCharsets.UTF_8))
    hash.update(255.toByte)
    for(field <- ContentNegotiation.headers) {
      hash.update(field.getBytes(StandardCharsets.UTF_8))
      hash.update(254.toByte)
      for(elem <- req.getHeaders(field).asScala.asInstanceOf[Iterator[String]]) {
        hash.update(elem.getBytes(StandardCharsets.UTF_8))
        hash.update(254.toByte)
      }
      hash.update(255.toByte)
    }
    hash.digest()
  }


  // TODO this method doesnt seem to have a use? determine and delete if we can.
  def response(result: RowDAO.Result): HttpResponse = {
    // TODO: Negotiate content-type
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> Json(value)
        // TODO other cases have not been implemented
      case _@x =>
        log.warn("case is NOT implemented")
        ???
    }
  }

  def rowResponse(req: HttpServletRequest, result: RowDAO.Result): HttpResponse = {
    // TODO: Negotiate content-type
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> Json(value)
      case RowDAO.RowNotFound(value) =>
        SodaUtils.response(req, SodaErrors.RowNotFound(value))
      case RowDAO.RowPrimaryKeyIsNonexistentOrNull(value) =>
        SodaUtils.response(req, SodaErrors.RowPrimaryKeyNonexistentOrNull(value))
      case RowDAO.UnknownColumn(columnName) =>
        SodaUtils.response(req, SodaErrors.RowColumnNotFound(columnName))
      case RowDAO.ComputationHandlerNotFound(typ) =>
        SodaUtils.response(req, SodaErrors.ComputationHandlerNotFound(typ))
      case RowDAO.CannotDeletePrimaryKey =>
        SodaUtils.response(req, SodaErrors.CannotDeletePrimaryKey)
      case RowDAO.RowNotAnObject(obj) =>
        SodaUtils.response(req, SodaErrors.UpsertRowNotAnObject(obj))
      case RowDAO.DatasetNotFound(dataset) =>
        SodaUtils.response(req, SodaErrors.DatasetNotFound(dataset))
      case RowDAO.SchemaOutOfSync =>
        SodaUtils.response(req, SodaErrors.SchemaInvalidForMimeType)
      case RowDAO.InvalidRequest(client, status, body) =>
        SodaUtils.response(req, SodaErrors.InternalError(s"Error from $client:", "code"  -> JNumber(status),
          "data" -> body))
      case RowDAO.QCError(status, qcErr) =>
        SodaUtils.response(req, SodaErrors.ErrorReportedByQueryCoordinator(status, qcErr))
      case RowDAO.InternalServerError(status, client, code, tag, data) =>
        SodaUtils.response(req, SodaErrors.InternalError(s"Error from $client:",
          "code"  -> JString(code),
          "data" -> JString(data),
          "tag"->JString(tag)))
      case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.response(req, SodaErrors.EtagPreconditionFailed)
      case RowDAO.RequestTimedOut(timeout) =>
        SodaUtils.response(req, SodaErrors.RequestTimedOut(timeout))
      case x =>
        log.warn("case is NOT implemented")
        SodaUtils.response(req, SodaErrors.InternalError(s"Error",
          "error" -> JString(x.toString)))
    }
  }

  type rowDaoFunc = (DatasetRecordLike, Iterator[RowUpdate]) => (RowDAO.UpsertResult => Unit) => Unit

  def upsertishFlow(req: HttpServletRequest,
                    response: HttpServletResponse,
                    requestId: RequestId.RequestId,
                    resourceName: ResourceName,
                    rows: Iterator[JValue],
                    f: rowDaoFunc,
                    reportFunc: (HttpServletResponse, Iterator[ReportItem]) => Unit) = {
    datasetDAO.getDataset(resourceName, None) match {
      case DatasetDAO.Found(datasetRecord) =>
        val transformer = new RowDataTranslator(requestId, datasetRecord, false)
        val transformedRows = transformer.transformClientRowsForUpsert(rows)
        f(datasetRecord, transformedRows)(UpsertUtils.handleUpsertErrors(req, response)(reportFunc))
      case DatasetDAO.DatasetNotFound(dataset) =>
        SodaUtils.response(req, SodaErrors.DatasetNotFound(resourceName))(response)
        // No other cases have to be implimented
      case x =>
        log.warn("case is NOT implemented")
        SodaUtils.response(req, SodaErrors.InternalError(s"Error",
          "error" -> JString(x.toString)))
    }
  }

  implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

  def extensions(s: String) = Exporter.exporterExtensions.contains(Exporter.canonicalizeExtension(s))

  def isConditionalGet(req: HttpRequest): Boolean = {
    req.header("If-None-Match").isDefined || req.dateTimeHeader("If-Modified-Since").isDefined
  }

  def domainMissingHandler(lostMetric: Metric) = {
    if (!metricProvider.isInstanceOf[NoopMetricProvider]) {
      log.warn(s"Domain ID not provided in request. Dropping metric - metricID: '${lostMetric.id}'")
    }
  }

  case class service(resourceName: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = { req: HttpRequest =>
      val domainId = req.header(domainIdHeader)
      def metric(metric: Metric) = metricProvider.add(domainId, metric)(domainMissingHandler)
      def metricByStatus(status: Int) = {
        if (status >= 400 && status < 500) metric(QueryErrorUser)
        else if (status >= 500 && status < 600) metric(QueryErrorInternal)
      }

      val start = System.currentTimeMillis
      try {
        val suffix = headerHash(req)
        val precondition = req.precondition.map(etagObfuscator.deobfuscate)
        def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
        precondition.filter(_.endsWith(suffix)) match {
          case Right(newPrecondition) =>
            req.negotiateContent match {
              case Some((mimeType, charset, language)) =>
                val exporter = Exporter.exportForMimeType(mimeType)
                val obfuscateId = reqObfuscateId(req)
                rowDAO.query(
                  resourceName.value,
                  newPrecondition.map(_.dropRight(suffix.length)),
                  req.dateTimeHeader("If-Modified-Since"),
                  Option(req.getParameter(qpQuery)).getOrElse(qpQueryDefault),
                  Option(req.getParameter(qpRowCount)),
                  Stage(req.getParameter(qpCopy)),
                  Option(req.getParameter(qpSecondary)),
                  Option(req.getParameter(qpNoRollup)).isDefined,
                  obfuscateId,
                  RequestId.getFromRequest(req),
                  Option(req.getHeader("X-Socrata-Fuse-Columns")),
                  Option(req.getParameter(qpQueryTimeoutSeconds)),
                  Option(req.getHeader("X-Socrata-Debug")).isDefined,
                  req.resourceScope) match {
                  case RowDAO.QuerySuccess(etags, truthVersion, truthLastModified, rollup, schema, rows) =>
                    metric(QuerySuccessMetric)
                    if (isConditionalGet(req)) {
                      metric(QueryCacheMiss)
                    }
                    val latencyMs = System.currentTimeMillis - start
                    if (rollup.isDefined) queryLatencyRollup += latencyMs
                    else                  queryLatencyNonRollup += latencyMs
                    val createHeader =
                      OK ~> // ContentType is handled in exporter
                        Header("Vary", ContentNegotiation.headers.mkString(",")) ~>
                        ETags(etags.map(prepareTag)) ~>
                        optionalHeader("Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Secondary-Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Data-Out-Of-Date", schema.dataVersion.map{ sv => (truthVersion > sv).toString }) ~>
                        optionalHeader(QueryCoordinatorClient.HeaderRollup, rollup) ~>
                        Header("X-SODA2-Truth-Last-Modified", truthLastModified.toHttpDate)
                    createHeader ~>
                      exporter.export(charset, schema, rows, singleRow = false, obfuscateId = obfuscateId,
                                      bom = Option(req.getParameter(qpBom)).map(_.toBoolean).getOrElse(false))
                  case RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
                    metric(QueryCacheHit)
                    SodaUtils.response(req, SodaErrors.ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))
                  case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
                    metric(QueryErrorUser)
                    SodaUtils.response(req, SodaErrors.EtagPreconditionFailed)
                  case RowDAO.DatasetNotFound(resourceName) =>
                    metric(QueryErrorUser)
                    SodaUtils.response(req, SodaErrors.DatasetNotFound(resourceName))
                  case RowDAO.RequestTimedOut(timeout) =>
                    metric(QueryErrorUser)
                    SodaUtils.response(req, SodaErrors.RequestTimedOut(timeout))
                  case RowDAO.QCError(status, qcErr) =>
                    metricByStatus(status)
                    if (Option(req.getHeader("X-Socrata-Collocate")) == Some("true")) {
                      collocateDataset(qcErr)
                    }
                    SodaUtils.response(req, SodaErrors.ErrorReportedByQueryCoordinator(status, qcErr))
                  case RowDAO.InvalidRequest(client, status, body) =>
                    metricByStatus(status)
                    SodaUtils.response(req, SodaErrors.InternalError(s"Error from $client:",
                      "code"  -> JNumber(status),
                      "data" -> body))
                  case RowDAO.InternalServerError(status, client, code, tag, data) =>
                    metricByStatus(status)
                    SodaUtils.response(req, SodaErrors.InternalError(s"Error from $client:",
                      "status" -> JNumber(status),
                      "code"  -> JString(code),
                      "data" -> JString(data),
                      "tag" -> JString(tag)))
                  case RowDAO.ServiceUnavailable =>
                    metricByStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE)
                    SodaUtils.response(req, SodaErrors.ServiceUnavailable)
                  case RowDAO.TooManyRequests =>
                    metricByStatus(429)
                    SodaUtils.response(req, SodaErrors.TooManyRequests)
                }
              case None =>
                metric(QueryErrorUser)
                // TODO better error
                NotAcceptable
            }
          case Left(Precondition.FailedBecauseNoMatch) =>
            metric(QueryErrorUser)
            SodaUtils.response(req, SodaErrors.EtagPreconditionFailed)
        }
      } catch {
        case e: Exception =>
          metric(QueryErrorInternal)
          throw e
      }
    }


    override def post = { req =>
      val requestId = RequestId.getFromRequest(req)
      RowUpdateOption.fromReq(req) match {
        case Right(options) =>
          { response =>
            upsertMany(req, response, requestId, rowDAO.upsert(user(req), _, _, requestId, options), allowSingleItem = true)
          }
        case Left((badParam, badValue)) =>
          SodaUtils.response(req, SodaErrors.BadParameter(badParam, badValue))
      }
    }

    override def put = { req => response =>
      val requestId = RequestId.getFromRequest(req)
      upsertMany(req, response, requestId, rowDAO.replace(user(req), _, _, requestId), allowSingleItem = false)
    }

    private def upsertMany(req: HttpRequest,
                           response: HttpServletResponse,
                           requestId: RequestId.RequestId,
                           f: rowDaoFunc,
                           allowSingleItem: Boolean) {
      InputUtils.jsonArrayValuesStream(req, maxRowSize, allowSingleItem) match {
        case Right((boundedIt, multiRows)) =>
          val processUpsertReport =
            if (multiRows) { UpsertUtils.writeUpsertResponse _ }
            else { UpsertUtils.writeSingleRowUpsertResponse(resourceName.value, export, req) _ }
          upsertishFlow(req, response, requestId, resourceName.value, boundedIt, f, processUpsertReport)
        case Left(err) =>
          SodaUtils.response(req, err, resourceName.value)(response)
      }
    }

    private def collocateDataset(qcErr: QueryCoordinatorError): Unit = {
      qcErr match {
        case QueryCoordinatorError("query.dataset.is-not-collocated", Some(description), data) =>
          (data("resource"), data("secondaries")) match {
            case (JString(resource), JArray(Seq(JString(sec), _*))) =>
              val resourceName = new ResourceName(resource)
              datasetDAO.getDataset(resourceName, None) match {
                case DatasetDAO.Found(datasetRecord) =>
                  val dsId = datasetRecord.systemId
                  val secId = SecondaryId(sec.split("\\.")(1))
                  log.info("collocate {} dataset {} in {}", resourceName, dsId.underlying, secId)
                  dc.propagateToSecondary(dsId, secId, Map.empty)
                case _ =>
              }
            case _ =>
          }
        case _ =>
      }
    }
  }

  case class rowService(resourceName: ResourceName, rowId: RowSpecifier) extends SodaResource {

    implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

    override def get = { req: HttpRequest =>
      val domainId = req.header(domainIdHeader)
      def metric(metric: Metric) = metricProvider.add(domainId, metric)(domainMissingHandler)
      try {
        val suffix = headerHash(req)
        val precondition = req.precondition.map(etagObfuscator.deobfuscate)
        def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
        precondition.filter(_.endsWith(suffix)) match {
          case Right(newPrecondition) =>
            // not using req.negotiateContent because we can't assume `.' signifies an extension
            contentNegotiation(req.accept, req.contentType, None, req.acceptCharset, req.acceptLanguage) match {
              case Some((mimeType, charset, language)) =>
                val exporter = Exporter.exportForMimeType(mimeType)
                val obfuscateId = reqObfuscateId(req)
                using(new ResourceScope) { resourceScope =>
                  rowDAO.getRow(
                    resourceName,
                    newPrecondition.map(_.dropRight(suffix.length)),
                    req.dateTimeHeader("If-Modified-Since"),
                    rowId,
                    Stage(req.getParameter(qpCopy)),
                    Option(req.getParameter(qpSecondary)),
                    Option(req.getParameter(qpNoRollup)).isDefined,
                    obfuscateId,
                    RequestId.getFromRequest(req),
                    Option(req.getHeader("X-Socrata-Fuse-Columns")),
                    Option(req.getParameter(qpQueryTimeoutSeconds)),
                    Option(req.getHeader("X-Socrata-Debug")).isDefined,
                    resourceScope) match {
                    case RowDAO.SingleRowQuerySuccess(etags, truthVersion, truthLastModified, schema, row) =>
                      metric(QuerySuccessMetric)
                      if (isConditionalGet(req)) {
                        metric(QueryCacheMiss)
                      }
                      val createHeader =
                        OK ~> // ContentType is handled in exporter
                        Header("Vary", ContentNegotiation.headers.mkString(",")) ~>
                        ETags(etags.map(prepareTag)) ~>
                        optionalHeader("Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Data-Out-Of-Date", schema.dataVersion.map{ sv => (truthVersion > sv).toString }) ~>
                        optionalHeader("X-SODA2-Secondary-Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        Header("X-SODA2-Truth-Last-Modified", truthLastModified.toHttpDate)
                      createHeader ~>
                        exporter.export(charset, schema, Iterator.single(row), singleRow = true, obfuscateId = obfuscateId,
                                        bom = Option(req.getParameter(qpBom)).map(_.toBoolean).getOrElse(false))
                    case RowDAO.RowNotFound(row) =>
                      metric(QueryErrorUser)
                      SodaUtils.response(req, SodaErrors.RowNotFound(row))
                    case RowDAO.RowPrimaryKeyIsNonexistentOrNull(row) =>
                      metric(QueryErrorUser)
                      SodaUtils.response(req, SodaErrors.RowPrimaryKeyNonexistentOrNull(row))
                    case RowDAO.DatasetNotFound(resourceName) =>
                      metric(QueryErrorUser)
                      SodaUtils.response(req, SodaErrors.DatasetNotFound(resourceName))
                    case RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
                      metric(QueryCacheHit)
                      SodaUtils.response(req, SodaErrors.ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))
                    case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
                      metric(QueryErrorUser)
                      SodaUtils.response(req, SodaErrors.EtagPreconditionFailed)
                    case RowDAO.RequestTimedOut(timeout) =>
                      metric(QueryErrorUser)
                      SodaUtils.response(req, SodaErrors.RequestTimedOut(timeout))
                    case RowDAO.SchemaInvalidForMimeType =>
                      metric(QueryErrorUser)
                      SodaUtils.response(req, SodaErrors.SchemaInvalidForMimeType)
                  }
                }
              case None =>
                metric(QueryErrorUser)
                // TODO better error
                NotAcceptable
            }
          case Left(Precondition.FailedBecauseNoMatch) =>
            metric(QueryErrorUser)
            SodaUtils.response(req, SodaErrors.EtagPreconditionFailed)
        }
      } catch {
        case e: Exception =>
          metric(QueryErrorInternal)
          throw e
      }
    }

    override def post = { req => response =>
      val requestId = RequestId.getFromRequest(req)
      InputUtils.jsonSingleObjectStream(req, maxRowSize) match {
        case Right(rowJVal) =>
          upsertishFlow(req, response, requestId, resourceName, Iterator.single(rowJVal),
                        rowDAO.upsert(user(req), _, _, requestId), UpsertUtils.writeUpsertResponse)
        case Left(err) =>
          SodaUtils.response(req, err, resourceName)(response)
      }
    }

    override def delete = { req => response =>
      rowDAO.deleteRow(user(req), resourceName, rowId, RequestId.getFromRequest(req))(
                       UpsertUtils.handleUpsertErrors(req, response)(UpsertUtils.writeUpsertResponse))
    }
  }

  private def reqObfuscateId(req: HttpRequest): Boolean =
    !Option(req.getParameter(qpObfuscateId)).exists(_ == "false")
}

object Resource {

  // Query Parameters
  val qpQuery = "$query"
  val qpRowCount = "$$row_count"
  val qpCopy = "$$copy" // Query parameter for copy.  Optional, "latest", "published", "unpublished"
  val qpSecondary = "$$store"
  val qpNoRollup = "$$no_rollup"
  val qpObfuscateId = "$$obfuscate_id" // for OBE compatibility - use false
  val qpBom = "$$bom"
  val qpQueryTimeoutSeconds = "queryTimeoutSeconds"

  val qpQueryDefault = "select *"
}

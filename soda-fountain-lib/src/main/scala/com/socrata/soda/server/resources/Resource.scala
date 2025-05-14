package com.socrata.soda.server.resources

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.JavaConverters._
import scala.language.existentials
import com.rojoma.json.v3.ast.{JArray, JNumber, JString, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.v2._
import com.socrata.http.common.util.ContentNegotiation
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.http.server.util._
import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, RowUpdate, RowUpdateOption}
import com.socrata.soda.clients.querycoordinator.{QueryCoordinatorClient, QueryCoordinatorError}
import com.socrata.soda.server.{responses => SodaErrors, _}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.export.Exporter
import com.socrata.soda.server.highlevel.{DatasetDAO, RowDAO, RowDataTranslator}
import com.socrata.soda.server.id.{ResourceName, RollupName, RowSpecifier, SecondaryId}
import com.socrata.soda.server.metrics.{MetricProvider, NoopMetricProvider}
import com.socrata.soda.server.metrics.Metrics.{QuerySuccess => QuerySuccessMetric, _}
import com.socrata.soda.server.persistence.DatasetRecordLike
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.wiremodels.InputUtils
import com.socrata.soql.stdlib.Context
import com.socrata.thirdparty.metrics.Metrics

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * Resource: services for upserting, deleting, and querying dataset rows.
 */
case class Resource(etagObfuscator: ETagObfuscator, maxRowSize: Long, metricProvider: MetricProvider, export: Export) extends Metrics {
  import Resource._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  // Unlike the global dispatch latency, these are for measuring only successful Query requests
  val queryLatencyNonRollup = metrics.histogram("query-latency-ms-nonrollup")
  val queryLatencyRollup = metrics.histogram("query-latency-ms-rollup")

  val domainIdHeader = "X-SODA2-Domain-Id"
  val lensUidHeader = "X-Socrata-Lens-Uid"

  val headerHashAlg = "SHA1"
  val headerHashLength = MessageDigest.getInstance(headerHashAlg).getDigestLength
  def headerHash(req: SodaRequest) = {
    val hash = MessageDigest.getInstance(headerHashAlg)
    hash.update(req.queryStr.toString.getBytes(StandardCharsets.UTF_8))
    hash.update(255.toByte)
    for(field <- ContentNegotiation.headers) {
      hash.update(field.getBytes(StandardCharsets.UTF_8))
      hash.update(254.toByte)
      for(elem <- req.headers(field)) {
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
        SodaUtils.response(req, SodaErrors.SchemaOutOfSync)
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

  def upsertishFlow(req: SodaRequest,
                    response: HttpServletResponse,
                    resourceName: ResourceName,
                    rows: Iterator[JValue],
                    f: rowDaoFunc,
                    reportFunc: (HttpServletResponse, RowDAO.StreamSuccess) => Unit) = {
    req.datasetDAO.getDataset(resourceName, None) match {
      case DatasetDAO.Found(datasetRecord) =>
        val obfuscateId = req.queryParameter(qpObfuscateId).map(java.lang.Boolean.parseBoolean(_)).getOrElse(true)
        val transformer = new RowDataTranslator(datasetRecord, false, obfuscateId)
        val transformedRows = transformer.transformClientRowsForUpsert(rows)
        f(datasetRecord, transformedRows)(UpsertUtils.handleUpsertErrors(req.httpRequest, response)(reportFunc))
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

  def isConditionalGet(req: SodaRequest): Boolean = {
    req.header("If-None-Match").isDefined || req.dateTimeHeader("If-Modified-Since").isDefined
  }

  def domainMissingHandler(lostMetric: Metric) = {
    if (!metricProvider.isInstanceOf[NoopMetricProvider]) {
      log.warn(s"Domain ID not provided in request. Dropping metric - metricID: '${lostMetric.id}'")
    }
  }

  class MetricContext(req: SodaRequest) {
    val domainId = req.header(domainIdHeader)
    def metric(metric: Metric) = metricProvider.add(domainId, metric)(domainMissingHandler)
  }

  case class service(resourceName: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def query = { req: SodaRequest =>
      val metricContext = new MetricContext(req)
      InputUtils.jsonSingleObjectStream(req.httpRequest, Long.MaxValue) match {
        case Right(obj) =>
          JsonDecode.fromJValue[Map[String, String]](obj) match {
            case Right(body) =>
              log.info("Request parameters: {}", JsonUtil.renderJson(obj, pretty=false))
              getlike(req, body.get _, metricContext)
            case Left(err) =>
              log.info("Body wasn't a map of strings: {}", err.english)
              metricContext.metric(QueryErrorUser)
              BadRequest // todo: better error
          }
        case Left(err) =>
          metricContext.metric(QueryErrorUser)
          SodaUtils.response(req, err)
      }
    }

    override def get = { req: SodaRequest =>
      getlike(req, req.queryParameter, new MetricContext(req))
    }

    def getlike(req: SodaRequest, parameter: String => Option[String], metricContext: MetricContext): HttpResponse = {
      import metricContext._
      val lensUid = req.header(lensUidHeader)

      def metricByStatus(status: Int) = {
        if (status >= 400 && status < 500) metric(QueryErrorUser)
        else if (status >= 500 && status < 600) metric(QueryErrorInternal)
      }

      val start = System.currentTimeMillis
      try {
        val suffix = headerHash(req)
        val precondition = req.precondition.map(etagObfuscator.deobfuscate)
        def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
        def updatePrecondition(newPrecondition: Precondition) = {
          // If there were originally etags in the precondition we need to add an invalid one back in
          // so that lower services are aware that the request was made with the If-None-Match header
          if(precondition.isInstanceOf[IfNoneOf] && newPrecondition == NoPrecondition){
            emptyINMHeader
          } else {
            newPrecondition
          }
        }
        precondition.filter(_.endsWith(suffix)) match {
          case Right(newPrecondition) =>
            req.negotiateContent match {
              case Some((mimeType, charset, language)) =>
                val exporter = Exporter.exportForMimeType(mimeType)
                val obfuscateId = reqObfuscateId(parameter)
                req.rowDAO.query(
                  resourceName.value,
                  updatePrecondition(newPrecondition).map(_.dropRight(suffix.length)),
                  req.dateTimeHeader("If-Modified-Since"),
                  parameter(qpQuery).getOrElse(qpQueryDefault),
                  parameter(qpContext).fold(Context.empty) { ctxStr =>
                    JsonUtil.parseJson[Context](ctxStr).right.get
                  },
                  parameter(qpRowCount),
                  Stage(parameter(qpCopy)),
                  parameter(qpSecondary),
                  parameter(qpNoRollup).isDefined,
                  obfuscateId,
                  req.header("X-Socrata-Fuse-Columns"),
                  parameter(qpQueryTimeoutSeconds),
                  DebugInfo(
                    req.header("X-Socrata-Debug").isDefined,
                    req.header("X-Socrata-Explain").isDefined,
                    req.header("X-Socrata-Analyze").isDefined
                  ),
                  req.resourceScope,
                  lensUid) match {
                  case RowDAO.QuerySuccess(etags, truthVersion, truthLastModified, rollup, schema, rows) =>
                    metric(QuerySuccessMetric)
                    if (isConditionalGet(req)) {
                      metric(QueryCacheMiss)
                    }
                    val latencyMs = System.currentTimeMillis - start
                    if (rollup.isDefined) {
                      queryLatencyRollup += latencyMs
                      req.datasetDAO.markRollupAccessed(resourceName.value,new RollupName(rollup.get))
                    }
                    else {
                      queryLatencyNonRollup += latencyMs
                    }
                    val createHeader =
                      OK ~> // ContentType is handled in exporter
                        Header("Vary", ContentNegotiation.headers.mkString(",")) ~>
                        ETags(etags.map(prepareTag)) ~>
                        optionalHeader("Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Secondary-Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Data-Out-Of-Date", schema.dataVersion.map{ sv => (truthVersion > sv).toString }) ~>
                        optionalHeader("X-SODA2-Row-Count", schema.approximateRowCount.map(_.toString)) ~>
                        optionalHeader(QueryCoordinatorClient.HeaderRollup, rollup) ~>
                        Header("X-SODA2-Truth-Last-Modified", truthLastModified.toHttpDate)
                    createHeader ~>
                      exporter.export(None, charset, schema, None, rows, singleRow = false, obfuscateId = obfuscateId,
                                      bom = parameter(qpBom).map(_.toBoolean).getOrElse(false))
                  case RowDAO.InfoSuccess(_, body) =>
                    // Just drain the iterator into an array, this should never be large
                    OK ~> Json(JArray(body.toSeq))
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
                    if (req.header("X-Socrata-Collocate") == Some("true")) {
                      collocateDataset(req.datasetDAO, req.dataCoordinator, qcErr)
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

    override def expectedDataVersion(req: SodaRequest): Option[Long] = {
      // ok so upsert is annoying.  Doing it this way so we can keep
      // uniformity in the case where we're not abusing soda-java as a
      // soda-fountain client while also not doing major surgery on
      // soda-java to support setting the header the way everything
      // else does.
      super.expectedDataVersion(req).orElse {
        req.parseQueryParameterAs[Long]("expectedDataVersion").toOption.flatten
      }
    }

    override def post = { req =>
      RowUpdateOption.fromReq(req.httpRequest) match {
        case Right(options) =>
          { response =>
            upsertMany(req, response, req.rowDAO.upsert(user(req), _, expectedDataVersion(req), _, options), allowSingleItem = true)
          }
        case Left((badParam, badValue)) =>
          SodaUtils.response(req, SodaErrors.BadParameter(badParam, badValue))
      }
    }

    override def put = { req => response =>
      upsertMany(req, response, req.rowDAO.replace(user(req), _, expectedDataVersion(req), _), allowSingleItem = false)
    }

    private def upsertMany(req: SodaRequest,
                           response: HttpServletResponse,
                           f: rowDaoFunc,
                           allowSingleItem: Boolean) {
      InputUtils.jsonArrayValuesStream(req.httpRequest, maxRowSize, allowSingleItem) match {
        case Right((boundedIt, multiRows)) =>

          val processUpsertReport =
            if (multiRows) { UpsertUtils.writeUpsertResponse _ }
            else { UpsertUtils.writeSingleRowUpsertResponse(resourceName.value, export, req) _ }
          upsertishFlow(req, response, resourceName.value, boundedIt, f, processUpsertReport)
        case Left(err) =>
          SodaUtils.response(req, err, resourceName.value)(response)
      }
    }

    private def collocateDataset(datasetDAO: DatasetDAO, dc: DataCoordinatorClient, qcErr: QueryCoordinatorError): Unit = {
      qcErr match {
        case QueryCoordinatorError("query.dataset.is-not-collocated", description, data) =>
          (data("resource"), data("secondaries")) match {
            case (JString(resource), JArray(Seq(JString(sec), _*))) =>
              val resourceName = new ResourceName(resource)
              datasetDAO.getDataset(resourceName, None) match {
                case DatasetDAO.Found(datasetRecord) =>
                  val dsHandle = datasetRecord.handle
                  val secId = SecondaryId(sec.split("\\.")(1))
                  log.info("collocate {} dataset {} in {}", resourceName, dsHandle, secId)
                  dc.propagateToSecondary(dsHandle, secId, None)
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

    override def query = { req: SodaRequest =>
      val metricContext = new MetricContext(req)
      InputUtils.jsonSingleObjectStream(req.httpRequest, Long.MaxValue) match {
        case Right(obj) =>
          JsonDecode.fromJValue[Map[String, String]](obj) match {
            case Right(body) =>
              log.info("Request parameters: {}", JsonUtil.renderJson(obj, pretty=false))
              getlike(req, body.get _, metricContext)
            case Left(err) =>
              log.info("Body wasn't a map of strings: {}", err.english)
              metricContext.metric(QueryErrorUser)
              BadRequest // todo: better error
          }
        case Left(err) =>
          metricContext.metric(QueryErrorUser)
          SodaUtils.response(req, err)
      }
    }

    override def get = { req: SodaRequest =>
      getlike(req, req.queryParameter, new MetricContext(req))
    }

    def getlike(req: SodaRequest, parameter: String => Option[String], metricContext: MetricContext): HttpResponse = {
      import metricContext._
      val lensUid = req.header(lensUidHeader)

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
                val obfuscateId = reqObfuscateId(parameter)
                using(new ResourceScope) { resourceScope =>
                  req.rowDAO.getRow(
                    resourceName,
                    newPrecondition.map(_.dropRight(suffix.length)),
                    req.dateTimeHeader("If-Modified-Since"),
                    rowId,
                    Stage(parameter(qpCopy)),
                    parameter(qpSecondary),
                    parameter(qpNoRollup).isDefined,
                    obfuscateId,
                    req.header("X-Socrata-Fuse-Columns"),
                    parameter(qpQueryTimeoutSeconds),
                    DebugInfo(
                      req.header("X-Socrata-Debug").isDefined,
                      req.header("X-Socrata-Explain").isDefined,
                      req.header("X-Socrata-Analyze").isDefined
                    ),
                    resourceScope,
                    lensUid) match {
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
                        exporter.export(None, charset, schema, None, Iterator.single(row), singleRow = true, obfuscateId = obfuscateId,
                                        bom = parameter(qpBom).map(_.toBoolean).getOrElse(false))
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
      InputUtils.jsonSingleObjectStream(req.httpRequest, maxRowSize) match {
        case Right(rowJVal) =>
          upsertishFlow(req, response, resourceName, Iterator.single(rowJVal),
                        req.rowDAO.upsert(user(req), _, expectedDataVersion(req), _), UpsertUtils.writeUpsertResponse)
        case Left(err) =>
          SodaUtils.response(req, err, resourceName)(response)
      }
    }

    override def delete = { req => response =>
      req.rowDAO.deleteRow(user(req), resourceName, expectedDataVersion(req), rowId)(
                           UpsertUtils.handleUpsertErrors(req.httpRequest, response)(UpsertUtils.writeUpsertResponse))
    }
  }

  private def reqObfuscateId(parameter: String => Option[String]): Boolean =
    !parameter(qpObfuscateId).exists(_ == "false")
}

object Resource {

  // Query Parameters
  val qpQuery = "$query"
  val qpContext = "$$context"
  val qpRowCount = "$$row_count"
  val qpCopy = "$$copy" // Query parameter for copy.  Optional, "latest", "published", "unpublished"
  val qpSecondary = "$$store"
  val qpNoRollup = "$$no_rollup"

  // To use plain row id, update both truth and pg secondary for the dataset in metadb as follow:
  //   1. update dataset_map set obfuscation_key = E'\\000' where resource_name = '_four-by-four'
  //   2. pass in $$obfuscate_id=false in both read and write (upsert) in resource/_four-by-four?$$obfuscate_id=false
  val qpObfuscateId = "$$obfuscate_id" // for OBE compatibility - use false
  val qpBom = "$$bom"
  val qpQueryTimeoutSeconds = "queryTimeoutSeconds"

  val qpQueryDefault = "select *"

  val emptyINMHeader = IfNoneOf(Seq(WeakEntityTag(Array(0))))
}

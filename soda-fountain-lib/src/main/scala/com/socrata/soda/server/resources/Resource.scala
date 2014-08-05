package com.socrata.soda.server.resources

import com.rojoma.json.ast.JValue
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.ContentNegotiation
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.http.server.util.{Precondition, EntityTag}
import com.socrata.soda.clients.datacoordinator.RowUpdate
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{OtherReportItem, UpsertReportItem}
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.{errors => SodaErrors}
import com.socrata.soda.server.errors.{SchemaInvalidForMimeType, SodaError}
import com.socrata.soda.server.export.Exporter
import com.socrata.soda.server.highlevel.{RowDataTranslator, RowDAO}
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.highlevel.RowDataTranslator._
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.id.RowSpecifier
import com.socrata.soda.server.persistence.{MinimalDatasetRecord, NameAndSchemaStore}
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.wiremodels.InputUtils
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.soda.server.metrics.{NoopMetricProvider, MetricProvider}
import com.socrata.soda.server.metrics.Metrics.{ QuerySuccess => QuerySuccessMetric } // conflict with RowDAO.QuerySuccess
import com.socrata.soda.server.metrics.Metrics._
/**
 * Resource: services for upserting, deleting, and querying dataset rows.
 */
case class Resource(rowDAO: RowDAO,
                    store: NameAndSchemaStore,
                    etagObfuscator: ETagObfuscator,
                    maxRowSize: Long,
                    cc: ComputedColumnsLike,
                    metricProvider: MetricProvider) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

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
      for(elem <- req.headers(field)) {
        hash.update(elem.getBytes(StandardCharsets.UTF_8))
        hash.update(254.toByte)
      }
      hash.update(255.toByte)
    }
    hash.digest()
  }

  def response(result: RowDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> SodaUtils.JsonContent(value)
    }
  }

  def rowResponse(req: HttpServletRequest, result: RowDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.Success(code, value) =>
        Status(code) ~> SodaUtils.JsonContent(value)
      case RowDAO.RowNotFound(value) =>
        SodaUtils.errorResponse(req, SodaErrors.RowNotFound(value))
    }
  }

  type rowDaoFunc = (MinimalDatasetRecord, Iterator[RowUpdate]) => RowDAO.UpsertResult

  def upsertishFlow(req: HttpServletRequest,
                    response: HttpServletResponse,
                    resourceName: ResourceName,
                    rows: Iterator[JValue],
                    f: rowDaoFunc) = {
      store.translateResourceName(resourceName) match {
        case Some(datasetRecord) =>
          val transformer = new RowDataTranslator(datasetRecord, false)
          val transformedRows = transformer.transformClientRowsForUpsert(cc, rows)
          UpsertUtils.handleUpsertErrors(req, response, resourceName) {
            f(datasetRecord, transformedRows)
          }
        case None =>
          SodaUtils.errorResponse(req, SodaErrors.DatasetNotFound(resourceName))(response)
      }
  }

  implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

  def extensions(s: String) = Exporter.exporterExtensions.contains(Exporter.canonicalizeExtension(s))

  def isConditionalGet(req: HttpServletRequest): Boolean = {
    req.header("If-None-Match").isDefined || req.dateTimeHeader("If-Modified-Since").isDefined
  }

  def domainMissingHandler(lostMetric: Metric) = {
    if (!metricProvider.isInstanceOf[NoopMetricProvider]) {
      log.warn(s"Domain ID not provided in request. Dropping metric - metricID: '${lostMetric.id}'")
    }
  }

  case class service(resourceName: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = { req: HttpServletRequest => response: HttpServletResponse =>
      val domainId = req.header(domainIdHeader)
      def metric(metric: Metric) = metricProvider.add(domainId, metric)(domainMissingHandler)
      try {
        val qpQuery = "$query" // Query parameter row count
        val qpRowCount = "$$row_count" // Query parameter row count
        val qpSecondary = "$$store"
        val suffix = headerHash(req)
        val precondition = req.precondition.map(etagObfuscator.deobfuscate)
        def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
        precondition.filter(_.endsWith(suffix)) match {
          case Right(newPrecondition) =>
            req.negotiateContent match {
              case Some((mimeType, charset, language)) =>
                val exporter = Exporter.exportForMimeType(mimeType)
                rowDAO.query(
                  resourceName.value,
                  newPrecondition.map(_.dropRight(suffix.length)),
                  req.dateTimeHeader("If-Modified-Since"),
                  Option(req.getParameter(qpQuery)).getOrElse("select *"),
                  Option(req.getParameter(qpRowCount)),
                  Option(req.getParameter(qpSecondary))
                ) match {
                  case RowDAO.QuerySuccess(etags, truthVersion, truthLastModified, schema, rows) =>
                    metric(QuerySuccessMetric)
                    if (isConditionalGet(req)) {
                      metric(QueryCacheMiss)
                    }
                    val createHeader =
                      OK ~>
                        ContentType(mimeType.toString) ~>
                        Header("Vary", ContentNegotiation.headers.mkString(",")) ~>
                        ETags(etags.map(prepareTag)) ~>
                        optionalHeader("Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Data-Out-Of-Date", schema.dataVersion.map{ sv => (truthVersion > sv).toString }) ~>
                        Header("X-SODA2-Truth-Last-Modified", truthLastModified.toHttpDate)
                    createHeader(response)
                    exporter.export(response, charset, schema, rows)
                  case RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
                    metric(QueryCacheHit)
                    SodaUtils.errorResponse(req, SodaErrors.ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))(response)
                  case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
                    metric(QueryErrorUser)
                    SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
                  case RowDAO.DatasetNotFound(resourceName) =>
                    metric(QueryErrorUser)
                    SodaUtils.errorResponse(req, SodaErrors.DatasetNotFound(resourceName))(response)
                  case RowDAO.InvalidRequest(code, body) =>
                    if (code >= 400 && code < 500) metric(QueryErrorUser)
                    else if (code >= 500 && code < 600) metric(QueryErrorInternal)

                    SodaError.QueryCoordinatorErrorCodec.decode(body) match {
                      case Some(qcError) =>
                        val err = SodaErrors.ErrorReportedByQueryCoordinator(code, qcError)
                        SodaUtils.errorResponse(req, err)(response)
                      case _ =>
                        SodaUtils.errorResponse(req, SodaErrors.InternalError("Cannot parse error from QC"))(response)
                    }
                }
              case None =>
                metric(QueryErrorUser)
                // TODO better error
                NotAcceptable(response)
            }
          case Left(Precondition.FailedBecauseNoMatch) =>
            metric(QueryErrorUser)
            SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
        }
      } catch {
        case e: Exception =>
          metric(QueryErrorInternal)
          throw e
      }
    }

    override def post = { req => response =>
      upsertMany(req, response, rowDAO.upsert(user(req), _, _))
    }

    override def put = { req => response =>
      upsertMany(req, response, rowDAO.replace(user(req), _, _))
    }

    private def upsertMany(req: HttpServletRequest, response: HttpServletResponse, f: rowDaoFunc) {
      InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
        case Right(boundedIt) =>
          upsertishFlow(req, response, resourceName.value, boundedIt, f)
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName.value)(response)
      }
    }
  }

  case class rowService(resourceName: ResourceName, rowId: RowSpecifier) extends SodaResource {

    implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

    override def get = { req: HttpServletRequest => response: HttpServletResponse =>
      val domainId = req.header(domainIdHeader)
      def metric(metric: Metric) = metricProvider.add(domainId, metric)(domainMissingHandler)
      try {
        val suffix = headerHash(req)
        val qpSecondary = "$$store"
        val precondition = req.precondition.map(etagObfuscator.deobfuscate)
        def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
        precondition.filter(_.endsWith(suffix)) match {
          case Right(newPrecondition) =>
            // not using req.negotiateContent because we can't assume `.' signifies an extension
            contentNegotiation(req.accept, req.contentType, None, req.acceptCharset, req.acceptLanguage) match {
              case Some((mimeType, charset, language)) =>
                val exporter = Exporter.exportForMimeType(mimeType)
                  rowDAO.getRow(
                    resourceName,
                    exporter.validForSchema,
                    newPrecondition.map(_.dropRight(suffix.length)),
                    req.dateTimeHeader("If-Modified-Since"),
                    rowId,
                    Option(req.getParameter(qpSecondary))
                  ) match {
                    case RowDAO.SingleRowQuerySuccess(etags, truthVersion, truthLastModified, schema, row) =>
                      metric(QuerySuccessMetric)
                      if (isConditionalGet(req)) {
                        metric(QueryCacheMiss)
                      }
                      val createHeader =
                        OK ~>
                        ContentType(mimeType.toString) ~>
                        Header("Vary", ContentNegotiation.headers.mkString(",")) ~>
                        ETags(etags.map(prepareTag)) ~>
                        optionalHeader("Last-Modified", schema.lastModified.map(_.toHttpDate)) ~>
                        optionalHeader("X-SODA2-Data-Out-Of-Date", schema.dataVersion.map{ sv => (truthVersion > sv).toString }) ~>
                        Header("X-SODA2-Truth-Last-Modified", truthLastModified.toHttpDate)
                      createHeader(response)
                      exporter.export(response, charset, schema, Iterator.single(row), singleRow = true)
                    case RowDAO.RowNotFound(row) =>
                      metric(QueryErrorUser)
                      SodaUtils.errorResponse(req, SodaErrors.RowNotFound(row))(response)
                    case RowDAO.DatasetNotFound(resourceName) =>
                      metric(QueryErrorUser)
                      SodaUtils.errorResponse(req, SodaErrors.DatasetNotFound(resourceName))(response)
                    case RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
                      metric(QueryCacheHit)
                      SodaUtils.errorResponse(req, SodaErrors.ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))(response)
                    case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
                      metric(QueryErrorUser)
                      SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
                    case RowDAO.SchemaInvalidForMimeType =>
                      metric(QueryErrorUser)
                      SodaUtils.errorResponse(req, SchemaInvalidForMimeType)
                  }
              case None =>
                metric(QueryErrorUser)
                // TODO better error
                NotAcceptable(response)
            }
          case Left(Precondition.FailedBecauseNoMatch) =>
            metric(QueryErrorUser)
            SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
        }
      } catch {
        case e: Exception =>
          metric(QueryErrorInternal)
          throw e
      }
    }

    override def post = { req => response =>
      InputUtils.jsonSingleObjectStream(req, maxRowSize) match {
        case Right(rowJVal) =>
          upsertishFlow(req, response, resourceName, Iterator.single(rowJVal), rowDAO.upsert(user(req), _, _))
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName)(response)
      }
    }

    override def delete = { req => response =>
      UpsertUtils.upsertResponse(req, response)(rowDAO.deleteRow(user(req), resourceName, rowId))
    }
  }
}

package com.socrata.soda.server.resources

import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.ContentNegotiation
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.http.server.util.{Precondition, EntityTag}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{OtherReportItem, UpsertReportItem}
import com.socrata.soda.clients.datacoordinator.{RowUpdate, DeleteRow, UpsertRow}
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.computation.ComputedColumns
import com.socrata.soda.server.{errors => SodaErrors}
import com.socrata.soda.server.errors.SodaError
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

/**
 * Resource: services for upserting, deleting, and querying dataset rows.
 */
case class Resource(rowDAO: RowDAO,
                    store: NameAndSchemaStore,
                    etagObfuscator: ETagObfuscator,
                    maxRowSize: Long,
                    cc: ComputedColumns[_]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Resource])

  val headerHashAlg = "SHA1"
  val headerHashLength = MessageDigest.getInstance(headerHashAlg).getDigestLength
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

  def upsertResponse(request: HttpServletRequest, response: HttpServletResponse)(result: RowDAO.UpsertResult) {
    log.info("TODO: Negotiate content-type")
    result match {
      case RowDAO.StreamSuccess(report) =>
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType(SodaUtils.jsonContentTypeUtf8) // TODO: negotiate charset too
        using(response.getWriter) { w =>
          // TODO: send actual response
          val jw = new CompactJsonWriter(w)
          w.write('[')
          var wroteOne = false
          while(report.hasNext) {
            report.next() match {
              case UpsertReportItem(items) =>
                while(items.hasNext) {
                  if(wroteOne) w.write(',')
                  else wroteOne = true
                  jw.write(items.next())
                }
              case OtherReportItem => // nothing; probably shouldn't have occurred!
            }
          }
          w.write("]\n")
        }
      case mismatch : MaltypedData =>
        SodaUtils.errorResponse(request, new SodaErrors.ColumnSpecMaltyped(mismatch.column.name, mismatch.expected.name.name, mismatch.got))(response)
      case RowDAO.RowNotFound(rowSpecifier) =>
        SodaUtils.errorResponse(request, SodaErrors.RowNotFound(rowSpecifier))(response)
      case RowDAO.UnknownColumn(columnName) =>
        SodaUtils.errorResponse(request, SodaErrors.RowColumnNotFound(columnName))(response)
      case RowDAO.ComputationHandlerNotFound(typ) =>
        SodaUtils.errorResponse(request, SodaErrors.ComputationHandlerNotFound(typ))(response)
      case RowDAO.ComputedColumnNotWritable(columnName) =>
        SodaUtils.errorResponse(request, SodaErrors.ComputedColumnNotWritable(columnName))(response)
    }
  }

  implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

  def extensions(s: String) = Exporter.exporterExtensions.contains(Exporter.canonicalizeExtension(s))

  case class service(resourceName: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = { req: HttpServletRequest => response: HttpServletResponse =>
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
                  SodaUtils.errorResponse(req, SodaErrors.ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))(response)
                case RowDAO.DatasetNotFound(resourceName) =>
                  SodaUtils.errorResponse(req, SodaErrors.DatasetNotFound(resourceName))(response)
                case RowDAO.InvalidRequest(code, body) =>
                  SodaError.QueryCoordinatorErrorCodec.decode(body) match {
                    case Some(qcError) =>
                      val err = SodaErrors.ErrorReportedByQueryCoordinator(code, qcError)
                      SodaUtils.errorResponse(req, err)(response)
                    case _ =>
                      SodaUtils.errorResponse(req, SodaErrors.InternalError("Cannot parse error from QC"))(response)
                  }
              }
            case None =>
              // TODO better error
              NotAcceptable(response)
          }
        case Left(Precondition.FailedBecauseNoMatch) =>
          SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
      }
    }

    override def post = { req => response =>
      upsertishFlow(req, response, rowDAO.upsert)
    }

    override def put = { req => response =>
      upsertishFlow(req, response, rowDAO.replace)
    }

    type rowDaoFunc = (String, MinimalDatasetRecord, Iterator[RowUpdate]) => (RowDAO.UpsertResult => Unit) => Unit

    import ComputedColumns._

    private def upsertishFlow(req: HttpServletRequest, response: HttpServletResponse, f: rowDaoFunc) {
      InputUtils.jsonArrayValuesStream(req, maxRowSize) match {
        case Right(boundedIt) =>
          // Now look up the dataset schema to see if we need to compute columns
          store.translateResourceName(resourceName.value) match {
            case Some(datasetRecord) =>
              val trans = new RowDataTranslator(datasetRecord, ignoreUnknownColumns = false)
              val rowsAsSoql = boundedIt.map(trans.clientJsonToSoql)

              // Bail out of the whole upsert if any of the rows are invalid.
              // Do we eventually want to change this to upsert the valid rows and return
              // a summary of errors found?
              // We would need to first inform API users of any change in behavior.
              val validRows = rowsAsSoql.collect {
                case validUpsert: UpsertAsSoQL                               => validUpsert
                case validDelete: DeleteAsCJson                              => validDelete
                case MaltypedDataError(columnName, expected, got)            =>
                  return upsertResponse(req, response)(MaltypedData(columnName, expected, got))
                case UnknownColumnError(columnName)                          =>
                  return upsertResponse(req, response)(UnknownColumn(columnName))
                case DeleteNoPKError                                         =>
                  return upsertResponse(req, response)(DeleteWithoutPrimaryKey)
                case NotAnObjectOrSingleElementArrayError(obj)               =>
                  return upsertResponse(req, response)(RowNotAnObject(obj))
                case RowDataTranslator.ComputedColumnNotWritable(columnName) =>
                  return upsertResponse(req, response)(RowDAO.ComputedColumnNotWritable(columnName))
              }

              val computedColumns = cc.findComputedColumns(datasetRecord)
              val computedRows = cc.addComputedColumns(validRows, computedColumns)
              computedRows match {
                case ComputeSuccess(rows) =>
                  val rowUpdates = rows.map {
                    case UpsertAsSoQL(rowData) => trans.soqlToDataCoordinatorJson(rowData) match {
                      case UpsertAsCJson(row) => UpsertRow(row)
                      case UnknownColumnError(columnName) => return upsertResponse(req, response)(UnknownColumn(columnName))
                    }
                    case DeleteAsCJson(pk) => DeleteRow(pk)
                  }
                  f(user(req), datasetRecord, rowUpdates)(upsertResponse(req, response))
                case HandlerNotFound(typ) => upsertResponse(req, response)(ComputationHandlerNotFound(typ))
              }
            case None =>
              upsertResponse(req, response)(RowDAO.DatasetNotFound(resourceName.value))
          }
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName.value)(response)
      }
    }
  }

  case class rowService(resourceName: ResourceName, rowId: RowSpecifier) extends SodaResource {

    implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

    override def get = { req: HttpServletRequest => response: HttpServletResponse =>
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
                  SodaUtils.errorResponse(req, SodaErrors.RowNotFound(row))(response)
                case RowDAO.DatasetNotFound(resourceName) =>
                  SodaUtils.errorResponse(req, SodaErrors.DatasetNotFound(resourceName))(response)
                case RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
                  SodaUtils.errorResponse(req, SodaErrors.ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))(response)
                case RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
                  SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
                case RowDAO.SchemaInvalidForMimeType => NotAcceptable(response)
              }
            case None =>
              // TODO better error
              NotAcceptable(response)
          }
        case Left(Precondition.FailedBecauseNoMatch) =>
          SodaUtils.errorResponse(req, SodaErrors.EtagPreconditionFailed)(response)
      }
    }

    override def post = { req => response =>
      InputUtils.jsonSingleObjectStream(req, maxRowSize) match {
        case Right(rowJVal) =>
          store.translateResourceName(resourceName) match {
            case Some(datasetRecord) =>
              rowDAO.upsert(user(req), datasetRecord, Iterator.single(UpsertRow(rowJVal.fields)))(upsertResponse(req, response))
            case None =>
              SodaUtils.errorResponse(req, SodaErrors.DatasetNotFound(resourceName))(response)
          }
        case Left(err) =>
          SodaUtils.errorResponse(req, err, resourceName)(response)
      }
    }

    override def delete = { req => response =>
      rowDAO.deleteRow(user(req), resourceName, rowId)(upsertResponse(req, response))
    }
  }
}

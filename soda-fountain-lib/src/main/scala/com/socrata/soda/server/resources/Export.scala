package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.JString
import com.socrata.http.common.util.ContentNegotiation
import com.socrata.soda.server._
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.http.server.util.{EntityTag, Precondition, RequestId}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.responses._
import com.socrata.soda.server.export.Exporter
import com.socrata.soda.server.highlevel.ExportDAO.{CSchema, ColumnInfo}
import com.socrata.soda.server.highlevel.{ColumnSpecUtils, ExportDAO, ExportParam}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.socrata.soda.server.resources.Resource.qpObfuscateId
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

case class Export(etagObfuscator: ETagObfuscator) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Export])

  implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

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

  def export(resourceName: ResourceName, ext: Option[String])(req: SodaRequest): HttpResponse = {
    exportCopy(resourceName, "published", ext)(req)
  }

  def exportCopy(resourceName: ResourceName, copy: String, ext: Option[String])(req: SodaRequest): HttpResponse = {
    // Etags generated by this system are the obfuscation of the etag from upstream plus
    // the hash of the contents of the header fields naemd by ContentNegotiation.headers.
    // So, when we receive etags in an if-none-match from the client
    //   1. decrypt the tags
    //   2. extract our bit of the data
    //   3. hash our headers and compare, dropping the etag completely if the hash is different
    //   4. Passing the remaining (decrypted and hash-stripped) etags upstream.
    //
    // For if-match it's the same, only we KEEP the ones that match the hash (and if that eliminates
    // all of them, then we "expectation failed" before ever passing upward to the data-coordinator)
    val limit = req.queryParameter("limit").map { limStr =>
      try {
        limStr.toLong
      } catch {
        case e: NumberFormatException =>
          return SodaUtils.response(req, BadParameter("limit", limStr))
      }
    }

    val offset = req.queryParameter("offset").map { offStr =>
      try {
        offStr.toLong
      } catch {
        case e: NumberFormatException =>
          return SodaUtils.response(req, BadParameter("offset", offStr))
      }
    }

    val excludeSystemFields = req.queryParameter("exclude_system_fields").map { paramStr =>
      try {
        paramStr.toBoolean
      } catch {
        case e: Exception =>
          return SodaUtils.response(req, BadParameter("exclude_system_fields", paramStr))
      }
    }.getOrElse(true)

    // Excluding system columns is done by explicitly select all non-system columns.
    val reqColumns = req.queryParameter("columns").orElse(if (excludeSystemFields) Some("*") else None)

    // get only these columns, we expect these to be fieldnames comma separated items.
    val columnsOnly = reqColumns.map {
      paramStr =>
        try {
          req.exportDAO.lookupDataset(resourceName, Stage(Some(copy))) match {
            case Some(ds) => {
              val pkColumnId = ds.primaryKey
              val columns = ds.columns
              if (paramStr != "*") {
                val columnFields = paramStr.toLowerCase.split(",").toSeq.map(x => x.trim)
                // need to have the row-identifier included otherwise dc will fail
                // this may seem long winded but helps avoid double counting in case one of requested
                // columns is the row-identifier
                val pkColumn = columns.filter { c => c.id.underlying == pkColumnId.underlying}.map { c => c.fieldName.name}
                val columnFieldsWithRowIdSet = (columnFields ++ pkColumn).toSet
                val filtered = columns.filter { c => columnFieldsWithRowIdSet.contains(c.fieldName.name)}

                if (filtered.length != columnFieldsWithRowIdSet.size) {
                  return SodaUtils.response(req, BadParameter("could not find columns requested.", paramStr))
                }
                filtered
              } else {
                columns.filter(c => !ColumnSpecUtils.isSystemColumn(c.fieldName) || c.id.underlying == pkColumnId.underlying)
              }
            }
            case None =>
              return SodaUtils.response(req, DatasetNotFound(resourceName))
          }
        } catch {
          case e: Exception =>
            return SodaUtils.response(req, BadParameter("error locating columns requested", paramStr))
        }
    }.getOrElse(Seq.empty)

    val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")

    val sorted = req.queryParameter("sorted").map {
      case "true" => true
      case "false" => false
      case other => return SodaUtils.response(req, BadParameter("sorted", other))
    }.getOrElse(true)

    val rowId = req.queryParameter("row_id")

    val param = ExportParam(limit, offset, columnsOnly, ifModifiedSince, sorted, rowId)
    val fuseMap: Map[String, String] = req.header("X-Socrata-Fuse-Columns")
      .map(parseFuseColumnMap(_))
      .getOrElse(Map.empty)
    exportCopy(resourceName,
               copy,
               ext,
               excludeSystemFields,
               param,
               false,
               fuseMap)(req)
  }

  def exportCopy(resourceName: ResourceName,
                 copy: String,
                 ext: Option[String],
                 excludeSystemFields: Boolean,
                 param: ExportParam,
                 singleRow: Boolean,
                 fuseMap: Map[String, String])
                (req: SodaRequest) : HttpResponse = {
    // Etags generated by this system are the obfuscation of the etag from upstream plus
    // the hash of the contents of the header fields naemd by ContentNegotiation.headers.
    // So, when we receive etags in an if-none-match from the client
    //   1. decrypt the tags
    //   2. extract our bit of the data
    //   3. hash our headers and compare, dropping the etag completely if the hash is different
    //   4. Passing the remaining (decrypted and hash-stripped) etags upstream.
    //
    // For if-match it's the same, only we KEEP the ones that match the hash (and if that eliminates
    // all of them, then we "expectation failed" before ever passing upward to the data-coordinator)
    val suffix = headerHash(req)
    val precondition = req.precondition.map(etagObfuscator.deobfuscate)
    def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
    val obfuscateId = req.queryParameter(qpObfuscateId).map(java.lang.Boolean.parseBoolean(_)).getOrElse(true)
    precondition.filter(_.endsWith(suffix)) match {
      case Right(newPrecondition) =>
        val passOnPrecondition = newPrecondition.map(_.dropRight(suffix.length))
        req.negotiateContent match {
          case Some((mimeType, charset, language)) =>
            val exporter = Exporter.exportForMimeType(mimeType)
            req.exportDAO.export(resourceName,
                                 passOnPrecondition,
                                 copy,
                                 param,
                                 requestId = req.requestId,
                                 resourceScope = req.resourceScope) match {
              case ExportDAO.Success(fullSchema, newTag, fullRows) =>
                val headers = OK ~> Header("Vary", ContentNegotiation.headers.mkString(",")) ~> newTag.foldLeft(NoOp) { (acc, tag) =>
                  acc ~> ETag(prepareTag(tag))
                }
                // TODO: DC export always includes row id
                // When DC has the option to exclude row id,
                // move the work downstream to avoid tempering the row array.
                val isSystemRowId = (ci: ColumnInfo) => ci.fieldName.name == ":id"
                val (sysColumns, userColumns) = fullSchema.schema.partition(isSystemRowId)
                val sysColsStart = fullSchema.schema.indexWhere(isSystemRowId(_))
                val (schema: CSchema, rows) =
                  if (!excludeSystemFields || sysColumns.size == 0) (fullSchema, fullRows)
                  else (fullSchema.copy(schema = userColumns),
                    fullRows.map(row => row.take(sysColsStart) ++ row.drop(sysColsStart + sysColumns.size)))
                // TODO: determine whether tenant metrics is needed in export
                headers ~> exporter.export(charset, schema, rows, singleRow, obfuscateId, fuseMap = fuseMap)
              case ExportDAO.PreconditionFailed =>
                SodaUtils.response(req, EtagPreconditionFailed)
              case ExportDAO.NotModified(etags) =>
                SodaUtils.response(req, ResourceNotModified(etags.map(prepareTag),
                  Some(ContentNegotiation.headers.mkString(","))
                ))
              case ExportDAO.SchemaInvalidForMimeType =>
                SodaUtils.response(req, SchemaInvalidForMimeType)
              case ExportDAO.NotFound(x) =>
                SodaUtils.response(req, GeneralNotFoundError(x.toString()))
              case ExportDAO.InvalidRowId =>
                SodaUtils.response(req, InvalidRowId)
              case ExportDAO.InternalServerError(code, tag, data) =>
                SodaUtils.response(req, InternalError(tag,
                  "code"  -> JString(code),
                  "data" -> JString(data)
                ))
            }
          case None =>
            // TODO better error
            NotAcceptable
        }
      case Left(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.response(req, EtagPreconditionFailed)
    }
  }

  case class publishedService(resourceAndExt: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = export(resourceAndExt.value, resourceAndExt.extension.map(Exporter.canonicalizeExtension))
  }

  case class service(resource: ResourceName, copyAndExt: OptionallyTypedPathComponent[String]) extends SodaResource {
    override def get = exportCopy(resource, copyAndExt.value, copyAndExt.extension.map(Exporter.canonicalizeExtension))
  }

  def extensions(s: String) = Exporter.exporterExtensions.contains(Exporter.canonicalizeExtension(s))

  private def parseFuseColumnMap(s: String): Map[String, String] = {
    s.split(';')
      .map { item => item.split(',') }
      .map { case Array(a, b) => (a, b) }
      .toMap
  }
}

package com.socrata.soda.clients.querycoordinator


import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.util._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorError._

import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import org.apache.http.HttpStatus._
import org.joda.time.DateTime

trait HttpQueryCoordinatorClient extends QueryCoordinatorClient {
  def qchost : Option[RequestBuilder]
  val httpClient: HttpClient

  private[this] val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpQueryCoordinatorClient])

  private val qpDataset = "ds"
  private val qpQuery = "q"
  private val qpIdMap = "idMap"
  private val qpRowCount = "rowCount"
  private val qpCopy = "copy"
  private val secondaryStoreOverride = "store"
  private val qpNoRollup = "no_rollup"
  private val qpObfuscateId = "obfuscateId"

  def query[T](datasetId: DatasetId, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String,
    columnIdMap: Map[ColumnName, ColumnId], rowCount: Option[String],
    copy: Option[Stage], secondaryInstance:Option[String], noRollup: Boolean,
    obfuscateId: Boolean,
    extraHeaders: Map[String, String],
    rs: ResourceScope)(f: Result => T): T = {

    qchost match {
      case Some(host) =>
        val jsonizedColumnIdMap = JsonUtil.renderJson(columnIdMap.map { case(k,v) => k.name -> v.underlying})
        val params = List(
            qpDataset -> datasetId.underlying,
            qpQuery -> query,
            qpIdMap -> jsonizedColumnIdMap) ++
          copy.map(c => List(qpCopy -> c.name.toLowerCase)).getOrElse(Nil) ++ // Query coordinate needs publication stage in lower case.
          rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
          (if (noRollup) List(qpNoRollup -> "y") else Nil) ++
          (if (!obfuscateId) List(qpObfuscateId -> "false") else Nil) ++
          secondaryInstance.map(so => List(secondaryStoreOverride -> so)).getOrElse(Nil)
        log.debug("Query Coordinator request parameters: " + params)
        val request = host.addHeaders(PreconditionRenderer(precondition) ++
                                      ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate) ++
                                      extraHeaders).form(params)
        f(resultFrom(rs.open(httpClient.executeUnmanaged(request)), query, rs))
      case None => throw new Exception("could not connect to query coordinator")
    }
  }

  def resultFrom(response: Response, query: String, rs: ResourceScope): Result = {
    response.resultCode match {
      case SC_OK =>
        val jsonEventIt = response.jsonEvents()
        val jvIt = JsonArrayIterator[JValue](jsonEventIt)
        val umJvIt = rs.openUnmanaged(jvIt, Seq(response))
        Success(response.headers("ETag").map(EntityTagParser.parse(_)), response.headers(HeaderRollup).headOption, umJvIt)
      case SC_NOT_MODIFIED =>
        NotModified(response.headers("ETag").map(EntityTagParser.parse(_)))
      case SC_PRECONDITION_FAILED =>
        PreconditionFailed
      case status =>
        val r = response.value[QueryCoordinatorError]().right.toOption.getOrElse(
          throw new Exception(s"Response was JSON but not decodable as an error -  query: $query; code $status"))

        // TODO soda currently just pushes the body as a Json, so may not need this granularity here
        r match {
          case err: QueryCoordinatorError =>
            QueryCoordinatorResult(status, err)

          //            case qe : QueryError => qe match {
          //              case DataSourceUnavailable (datasetId) =>
          //                DataSourceUnavailableResult(qe.code, datasetId)
          //              case DoesNotExist(datasetId) =>
          //                DoesNotExistResult(qe.code, datasetId)
          //            }
          //            case  req: RequestError => req match {
          //              case NoDatasetSpecified() =>
          //                NoDatasetSpecifiedResult(req.code)
          //              case NoQuerySpecified() =>
          //                NoQuerySpecifiedResult(req.code)
          //              case UnknownColumnIds(columns) =>
          //                UnknownColumnIdsResult(req.code, columns)
          //              case RowLimitExceeded(limit) =>
          //                RowLimitExceededResult(req.code, limit)
          //            }
          //            case soql: SoqlError => soql match {
          //              case AggregateInUngroupedContext(data) =>
          //                AggregateInUngroupedContextResult(soql.code, data)
          //              case ColumnNotInGroupBys(data) =>
          //                ColumnNotInGroupBysResult(soql.code, data)
          //              case RepeatedException (data) =>
          //                RepeatedExceptionResult(soql.code, data)
          //              case DuplicateAlias(data) =>
          //                DuplicateAliasResult(soql.code, data)
          //              case NoSuchColumn(data) =>
          //                NoSuchColumnResult(soql.code, data)
          //              case CircularAliasDefinition(data) =>
          //                CircularAliasDefinitionResult(soql.code, data)
          //              case UnexpectedEscape(data) =>
          //                UnexpectedEscapeResult(soql.code, data)
          //              case BadUnicodeEscapeCharacter(data) =>
          //                BadUnicodeEscapeCharacterResult(soql.code, data)
          //              case UnicodeCharacterOutOfRange(data) =>
          //                UnicodeCharacterOutOfRangeResult(soql.code, data)
          //              case UnexpectedCharacter(data) =>
          //                UnexpectedCharacterResult(soql.code, data)
          //              case UnexpectedEOF(data) =>
          //                UnexpectedEOFResult(soql.code, data)
          //              case UnterminatedString(data) =>
          //                UnterminatedStringResult(soql.code, data)
          //              case BadParse(data) =>
          //                BadParseResult(soql.code, data)
          //              case NoSuchFunction(data) =>
          //                NoSuchFunctionResult(soql.code, data)
          //              case TypeMismatch(data) =>
          //                TypeMismatchResult(soql.code, data)
          //              case AmbiguousCall(data) =>
          //                AmbiguousCallResult(soql.code, data)
          //            }
          // Unknown
          case x =>
            val error = x.toString
            log.error(s"Unknown data coordinator status: $status;  error $error")
            InternalServerErrorResult(status, "unknown", tag, error)
        }
    }
  }





  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

}

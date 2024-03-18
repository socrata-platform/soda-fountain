package com.socrata.soda.clients.querycoordinator

import java.net.URLEncoder
import com.rojoma.json.v3.ast.{JNull, JNumber, JValue}
import com.rojoma.json.v3.util.{JsonArrayIterator, JsonUtil, AutomaticJsonEncode, OrJNull}
import com.rojoma.json.v3.util.OrJNull.implicits._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.client.exceptions.{ConnectFailed, ConnectTimeout, ReceiveTimeout, UnexpectedContentType}
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.util._
import com.socrata.soda.server.id.{DatasetInternalName, ColumnId}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorError._
import com.socrata.soda.server.{SodaUtils, ThreadLimiter}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.DatasetHandle
import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.stdlib.Context
import com.socrata.soql.stdlib.analyzer2.{Context => NewContext}
import com.socrata.soql.sql.Debug
import com.socrata.soql.util.GenericSoQLError
import org.apache.http.HttpStatus._
import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration

trait HttpQueryCoordinatorClient extends QueryCoordinatorClient {
  def qchost : Option[RequestBuilder]
  val httpClient: HttpClient
  val threadLimiter: ThreadLimiter

  val defaultReceiveTimeout: FiniteDuration

  // The threadlimiter wants to be able to log messages on behalf of this
  val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpQueryCoordinatorClient])

  private val qpDataset = "ds"
  private val qpQuery = "q"
  private val qpContext = "c"
  private val qpRowCount = "rowCount"
  private val qpCopy = "copy"
  private val secondaryStoreOverride = "store"
  private val qpNoRollup = "no_rollup"
  private val qpObfuscateId = "obfuscateId"
  private val qpQueryTimeoutSeconds = "queryTimeoutSeconds"
  private val qpLensUid = "lensUid"

  private def retrying[T](limit: Int)(f: => T): T = {
    def doRetry(count: Int, e: Exception): T = {
      if(count == limit) throw e
      else loop(count + 1)
    }
    def loop(count: Int): T = {
      try { f }
      catch {
        case e: ConnectTimeout => doRetry(count, e)
        case e: ConnectFailed => doRetry(count, e)
      }
    }
    loop(0)
  }

  def query[T](dataset: DatasetHandle, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String,
    context: Context,
    rowCount: Option[String],
    copy: Option[Stage], secondaryInstance:Option[String], noRollup: Boolean,
    obfuscateId: Boolean,
    extraHeaders: Map[String, String], queryTimeoutSeconds: Option[String],
    rs: ResourceScope, lensUid: Option[String])(f: Result => T): T = {

    val params = List(
      qpDataset -> dataset.datasetId.underlying,
      qpQuery -> query,
      qpContext -> JsonUtil.renderJson(context, pretty=false)) ++
      // when $$query_timeout_seconds is not given, always limit it to the default value - typically 10 minutes
      queryTimeoutSeconds.orElse(Some(defaultReceiveTimeout.toSeconds.toString)).map(qpQueryTimeoutSeconds -> _) ++
      copy.map(c => List(qpCopy -> c.name.toLowerCase)).getOrElse(Nil) ++ // Query coordinate needs publication stage in lower case.
      rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
      lensUid.map(uid => List(qpLensUid -> uid)).getOrElse(Nil) ++
      (if (noRollup) List(qpNoRollup -> "y") else Nil) ++
      (if (!obfuscateId) List(qpObfuscateId -> "false") else Nil) ++
      secondaryInstance.map(so => List(secondaryStoreOverride -> so)).getOrElse(Nil)
    log.debug("Query Coordinator request parameters: " + params)

    val result = {
      try {
        retrying(5) {
          qchost match {
            case Some(host) =>
              val rb = host.addHeaders(PreconditionRenderer(precondition) ++
                ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate) ++
                Map(SodaUtils.ResourceHeader -> URLEncoder.encode(dataset.resourceName.name, "UTF-8")) ++
                extraHeaders)
              val rbto = queryTimeoutSeconds match {
                case Some(timeout) =>
                  val timeoutMs = timeout.toFloat.toInt * 1000
                  rb.connectTimeoutMS(timeoutMs).receiveTimeoutMS(timeoutMs)
                case _ => rb
              }
              val request = rbto.form(params)
              threadLimiter.withThreadpool {
                httpClient.execute(request, rs)
              }
            case None =>
              throw new Exception("could not connect to query coordinator")
          }
        }
      } catch {
        case _: ReceiveTimeout =>
          val timeout: JValue  = qchost.flatMap(_.receiveTimeoutMS).map(JNumber(_)).getOrElse(JNull)
          return f(RequestTimedOut(timeout))
      }
    }
    f(resultFrom(result, query, rs))
  }

  def resultFrom(response: Response, query: String, rs: ResourceScope): Result = {
    response.resultCode match {
      case SC_OK =>
        val jsonEventIt = response.jsonEvents()
        val jvIt = JsonArrayIterator.fromEvents[JValue](jsonEventIt)
        val umJvIt = rs.openUnmanaged(jvIt, Seq(response))
        Success(response.headers("ETag").map(EntityTagParser.parse(_)), response.headers(HeaderRollup).headOption, response.headers("last-modified").head, umJvIt)
      case SC_NOT_MODIFIED =>
        NotModified(response.headers("ETag").map(EntityTagParser.parse(_)))
      case SC_PRECONDITION_FAILED =>
        PreconditionFailed
      case SC_REQUEST_TIMEOUT =>
        // if we can't read the timeout for some reason, pass through null but keep it as a RequestTimedOut
        response.value[RequestTimedOut]().right.toOption.getOrElse(RequestTimedOut(JNull))
      case SC_SERVICE_UNAVAILABLE =>
        ServiceUnavailable
      case 429 =>
        TooManyRequests
      case status =>
        try {
          val r = response.value[QueryCoordinatorError]().right.toOption.getOrElse(
            throw new Exception(s"Response was JSON but not decodable as an error -  query: $query; code $status"))
            r match {
              case err: QueryCoordinatorError =>
                QueryCoordinatorResult(status, err)
              case x =>
                val error = x.toString
                log.error(s"Unknown data coordinator status: $status;  error $error")
                InternalServerErrorResult(status, "unknown", tag, error)
            }
        } catch {
          case e: UnexpectedContentType =>
            throw new Exception(s"Query coordinator gave unexpected response of status $status and content-type ${response.contentType}.")
        }
    }
  }





  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

  @AutomaticJsonEncode
  private case class NewQueryBody(
    foundTables: analyzer2.UnparsedFoundTables[MetaTypes],
    locationSubcolumns: Seq[(DatabaseTableName, Seq[(DatabaseColumnName, Seq[OrJNull[DatabaseColumnName]])])],
    context: NewContext,
    rewritePasses: Seq[Seq[Pass]],
    allowRollups: Boolean,
    preserveSystemColumns: Boolean,
    debug: Option[Debug],
    queryTimeoutMS: Option[Long],
    store: Option[String]
  )

  override def newQuery(
    tables: analyzer2.UnparsedFoundTables[MetaTypes],
    locationSubcolumns: Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]],
    context: NewContext,
    rewritePasses: Seq[Seq[Pass]],
    allowRollups: Boolean,
    preserveSystemColumns: Boolean,
    debug: Option[Debug],
    queryTimeout: Option[FiniteDuration],
    store: Option[String],
    additionalHeaders: Seq[(String, String)],
    rs: ResourceScope
  ) = {
    val jValue = JsonEncode.toJValue(
      NewQueryBody(
        tables,
        locationSubcolumns.mapValues(_.mapValues(_.map(_.orJNull)).toSeq).toSeq,
        context,
        rewritePasses,
        allowRollups,
        preserveSystemColumns,
        debug,
        queryTimeout.map(_.toMillis),
        store
      )
    )

    retrying(5) {
      qchost match {
        case Some(host) =>
          val base = host.p("new-query").method("QUERY").addHeaders(additionalHeaders)
          val request = base.json(com.rojoma.json.v3.io.JValueEventIterator(jValue))
          threadLimiter.withThreadpool {
            val resp = httpClient.execute(request, rs)
            def interestingHeaders = Seq("etag", "last-modified", "content-type", "x-soda2-secondary")
            resp.resultCode match {
              case 200 =>
                val headers = interestingHeaders.foldLeft(Vector.empty[(String, String)]) { (acc, hdr) =>
                  resp.headers(hdr).foldLeft(acc) { case (acc, value) =>
                    acc :+ (hdr -> value)
                  }
                }
                rs.openUnmanaged(New.Success(headers, resp.inputStream()), transitiveClose = List(resp))

              case 304 =>
                val headers = interestingHeaders.foldLeft(Vector.empty[(String, String)]) { (acc, hdr) =>
                  resp.headers(hdr).foldLeft(acc) { case (acc, value) =>
                    acc :+ (hdr -> value)
                  }
                }
                rs.openUnmanaged(New.NotModified(headers, resp.inputStream()), transitiveClose = List(resp))

              case other =>
                val error = resp.value[GenericSoQLError[MetaTypes#ResourceNameScope]]() match {
                  case Right(value) => value
                  case Left(err) => throw new Exception("Invalid error response: " + err.english)
                }
                rs.close(resp)
                rs.openUnmanaged(New.Error(other, error))
            }
          }
        case None =>
          throw new Exception("could not connect to query coordinator")
      }
    }
  }
}

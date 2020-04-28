package com.socrata.soda.clients.querycoordinator


import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.client.exceptions.{ConnectFailed, ConnectTimeout}
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.util._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorError._
import com.socrata.soda.server.ThreadLimiter
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import org.apache.http.HttpStatus._
import org.joda.time.DateTime

trait HttpQueryCoordinatorClient extends QueryCoordinatorClient with ThreadLimiter {
  def qchost : Option[RequestBuilder]
  val httpClient: HttpClient

  // The threadlimiter wants to be able to log messages on behalf of this
  val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpQueryCoordinatorClient])

  private val qpDataset = "ds"
  private val qpQuery = "q"
  private val qpRowCount = "rowCount"
  private val qpCopy = "copy"
  private val secondaryStoreOverride = "store"
  private val qpNoRollup = "no_rollup"
  private val qpObfuscateId = "obfuscateId"
  private val qpQueryTimeoutSeconds = "queryTimeoutSeconds"

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

  def query[T](datasetId: DatasetId, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String,
    rowCount: Option[String],
    copy: Option[Stage], secondaryInstance:Option[String], noRollup: Boolean,
    obfuscateId: Boolean,
    extraHeaders: Map[String, String], queryTimeoutSeconds: Option[String],
    rs: ResourceScope)(f: Result => T): T = {

    val params = List(
      qpDataset -> datasetId.underlying,
      qpQuery -> query) ++
      queryTimeoutSeconds.map(qpQueryTimeoutSeconds -> _) ++
      copy.map(c => List(qpCopy -> c.name.toLowerCase)).getOrElse(Nil) ++ // Query coordinate needs publication stage in lower case.
      rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
      (if (noRollup) List(qpNoRollup -> "y") else Nil) ++
      (if (!obfuscateId) List(qpObfuscateId -> "false") else Nil) ++
      secondaryInstance.map(so => List(secondaryStoreOverride -> so)).getOrElse(Nil)
    log.debug("Query Coordinator request parameters: " + params)

    val result = retrying(5) {
      qchost match {
        case Some(host) =>
          val request = host.addHeaders(PreconditionRenderer(precondition) ++
                                        ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate) ++
                                        extraHeaders).form(params)
          withThreadpool{
            httpClient.execute(request, rs)
          }
        case None =>
          throw new Exception("could not connect to query coordinator")
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
    }
  }





  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

}

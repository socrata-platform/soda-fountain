package com.socrata.soda.clients.querycoordinator

import scala.io.{Codec, Source}

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.util._
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient._
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
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

  def query[T](datasetId: DatasetId, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String,
    columnIdMap: Map[ColumnName, ColumnId], rowCount: Option[String],
    copy: Option[Stage], secondaryInstance:Option[String], noRollup: Boolean, extraHeaders: Map[String, String],
    rs: ResourceScope)(f: Result => T): T = {
    import org.apache.http.HttpStatus._

    def resultFrom(response: Response): Result = {
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
        case code if code >= SC_BAD_REQUEST && code < SC_INTERNAL_SERVER_ERROR =>
          UserError(code, response.jValue())
        case _ =>
          val body = Source.fromInputStream(response.inputStream())(Codec(response.charset)).mkString("")
          throw new Exception(s"query: $query; response: $body")
      }
    }

    qchost match {
      case Some(host) =>
        val jsonizedColumnIdMap = JsonUtil.renderJson(columnIdMap.map { case(k,v) => k.name -> v.underlying})
        val params = List(qpDataset -> datasetId.underlying, qpQuery -> query, qpIdMap -> jsonizedColumnIdMap) ++
          copy.map(c => List(qpCopy -> c.name.toLowerCase)).getOrElse(Nil) ++ // Query coordinate needs publication stage in lower case.
          rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
          (if (noRollup) List(qpNoRollup -> "y") else Nil) ++
          secondaryInstance.map(so => List(secondaryStoreOverride -> so)).getOrElse(Nil)
        log.info("Query Coordinator request parameters: " + params)
        val request = host.addHeaders(PreconditionRenderer(precondition) ++
                                      ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate) ++
                                      extraHeaders).form(params)
        f(resultFrom(rs.open(httpClient.executeUnmanaged(request))))
      case None => throw new Exception("could not connect to query coordinator")
    }
  }
}

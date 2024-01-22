package com.socrata.soda.server.resources

import scala.collection.{mutable => scm}
import scala.concurrent.duration._

import java.io.{InputStream, BufferedInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.{JsonUtil, JsonArrayIterator, AutomaticJsonDecode}
import com.rojoma.simplearm.v2._

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.HttpResponse
import com.socrata.soql.analyzer2.{DatabaseTableName, DatabaseColumnName, LabelUniverse, types}
import com.socrata.soql.analyzer2
import com.socrata.soql.environment.ColumnName

import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.metrics.{MetricProvider, NoopMetricProvider}
import com.socrata.soda.server.metrics.Metrics.{QuerySuccess => QuerySuccessMetric, _}
import com.socrata.soda.server.{responses => SodaErrors, _}
import com.socrata.soda.server.wiremodels.{InputUtils, FoundTablesRequest}
import com.socrata.soda.server.wiremodels.metatypes.QueryMetaTypes
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{DatasetInternalName, ResourceName, ColumnId}
import com.socrata.soda.server.persistence.{DatasetRecord, NameAndSchemaStore}
import com.socrata.soda.server.util.{Lazy, DAOCache}
import com.socrata.thirdparty.metrics.Metrics

case class NewResource(etagObfuscator: ETagObfuscator, maxRowSize: Long, metricProvider: MetricProvider) extends Metrics {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[NewResource])
  val domainIdHeader = "X-SODA2-Domain-Id"

  def domainMissingHandler(lostMetric: Metric) = {
    if (!metricProvider.isInstanceOf[NoopMetricProvider]) {
      log.warn(s"Domain ID not provided in request. Dropping metric - metricID: '${lostMetric.id}'")
    }
  }

  class MetricContext(req: SodaRequest) {
    val domainId = req.header(domainIdHeader)
    def metric(metric: Metric) = metricProvider.add(domainId, metric)(domainMissingHandler)
  }

  case object service extends SodaResource {
    override def query = doQuery

    private def doQuery(req: SodaRequest): HttpResponse = {
      val metricContext = new MetricContext(req)

      InputUtils.jsonSingleObjectStream(req.httpRequest, Long.MaxValue) match {
        case Right(obj) =>
          JsonDecode.fromJValue[FoundTablesRequest](obj) match {
            case Right(reqData: FoundTablesRequest) =>
              log.debug("Received request {}", Lazy(JsonUtil.renderJson(reqData, pretty=true)))
              val cache = new DAOCache(req.nameAndSchemaStore)

              val qcFoundTables = reqData.tables.rewriteDatabaseNames[QueryCoordinatorClient.MetaTypes](
                cache.lookupTableName(_).getOrElse {
                  // TODO fail with "unknown table"
                  return BadRequest
                },
                cache.lookupColumnName(_, _).getOrElse {
                  // TODO fail with "unknown column"
                  return BadRequest
                }
              )

              val auxData = cache.buildAuxTableData(reqData.tables.allTableDescriptions).getOrElse {
                // TODO: fail with error
                return BadRequest
              }

              val relevantHeaders = Seq("if-none-match", "if-match", "if-modified-since").flatMap { h =>
                req.headers(h).map { h -> _ }
              }
              req.queryCoordinator.newQuery(
                qcFoundTables,
                auxData,
                reqData.context,
                reqData.rewritePasses,
                reqData.allowRollups,
                reqData.preserveSystemColumns,
                reqData.debug,
                reqData.queryTimeoutMS.map(_.milliseconds),
                reqData.store,
                relevantHeaders,
                req.resourceScope
              ) match {
                case QueryCoordinatorClient.New.Success(hdrs, stream) =>
                  val base = hdrs.foldLeft[HttpResponse](OK) { case (acc, (hdrName, hdrVal)) =>
                    acc ~> Header(hdrName, hdrVal)
                  }
                  base ~> Stream(stream)
                case QueryCoordinatorClient.New.NotModified(hdrs, stream) =>
                  val base = hdrs.foldLeft[HttpResponse](NotModified) { case (acc, (hdrName, hdrVal)) =>
                    acc ~> Header(hdrName, hdrVal)
                  }
                  base ~> Stream(stream)
                case QueryCoordinatorClient.New.Error(status, err) =>
                  Status(status) ~> Json(err)
              }
            case Left(err) =>
              log.info("Body wasn't a FoundTables: {}", err.english)
              metricContext.metric(QueryErrorUser)
              BadRequest // todo: better error
          }
        case Left(err) =>
          metricContext.metric(QueryErrorUser)
          SodaUtils.response(req, err)
      }
    }
  }
}

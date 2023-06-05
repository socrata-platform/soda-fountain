package com.socrata.soda.server.resources

import scala.collection.{mutable => scm}

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.JsonUtil

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.HttpResponse
import com.socrata.soql.analyzer2.{DatabaseTableName, DatabaseColumnName}
import com.socrata.soql.analyzer2
import com.socrata.soql.environment.ColumnName

import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.metrics.{MetricProvider, NoopMetricProvider}
import com.socrata.soda.server.metrics.Metrics.{QuerySuccess => QuerySuccessMetric, _}
import com.socrata.soda.server.{responses => SodaErrors, _}
import com.socrata.soda.server.wiremodels.{InputUtils, FoundTablesRequest}
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.{DatasetInternalName, ResourceName, ColumnId}
import com.socrata.soda.server.persistence.{DatasetRecord, NameAndSchemaStore}
import com.socrata.soda.server.util.Lazy
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

  private class DAOCache(store: NameAndSchemaStore) {
    private case class RelevantBits(
      internalName: DatasetInternalName,
      stage: Stage,
      schema: Map[ColumnName, ColumnId]
    )

    private val datasetCache = new scm.HashMap[(ResourceName, Stage), RelevantBits]

    private def getRecord(dtn: (ResourceName, Stage)): RelevantBits = {
      val (rn, stage) = dtn
      datasetCache.get(dtn) match {
        case Some(bits) =>
          bits
        case None =>
          store.lookupDataset(rn, Some(stage)) match {
            case Some(record) =>
              val bits =
                RelevantBits(
                  record.systemId,
                  stage,
                  record.columns.iterator.map { col =>
                    col.fieldName -> col.id
                  }.toMap
                )
              datasetCache += (rn, stage) -> bits
              bits
            case None =>
              // TODO
              throw new Exception("Was told to look up a dataset I don't know about: " + dtn)
          }
      }
    }

    def lookupTableName(dtn: DatabaseTableName[(ResourceName, Stage)]): DatabaseTableName[(DatasetInternalName, Stage)] = {
      val record = getRecord(dtn.name)
      DatabaseTableName((record.internalName, record.stage))
    }

    def lookupColumnName(dtn: DatabaseTableName[(ResourceName, Stage)], cn: DatabaseColumnName[ColumnName]): DatabaseColumnName[ColumnId] = {
      val record = getRecord(dtn.name)
      DatabaseColumnName(record.schema(cn.name))
    }
  }


  case object service extends SodaResource {
    override def query = { req: SodaRequest =>
      val metricContext = new MetricContext(req)

      InputUtils.jsonSingleObjectStream(req.httpRequest, Long.MaxValue) match {
        case Right(obj) =>
          JsonDecode.fromJValue[FoundTablesRequest](obj) match {
            case Right(reqData@FoundTablesRequest(tables, context, rewritePasses)) =>
              log.debug("Received request {}", Lazy(JsonUtil.renderJson(reqData, pretty=true)))
              val cache = new DAOCache(req.nameAndSchemaStore)
              val qcFoundTables = tables.rewriteDatabaseNames[QueryCoordinatorClient.MetaTypes](
                cache.lookupTableName,
                cache.lookupColumnName
              )
              val relevantHeaders = Seq("if-none-match", "if-match", "if-modified-since").flatMap { h =>
                req.headers(h).map { h -> _ }
              }
              req.queryCoordinator.newQuery(qcFoundTables, context, rewritePasses, relevantHeaders, req.resourceScope) match {
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

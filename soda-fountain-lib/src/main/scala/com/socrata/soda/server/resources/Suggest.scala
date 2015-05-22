package com.socrata.soda.server.resources

import java.net.URI
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast._
import com.socrata.http.client._
import com.socrata.http.client.exceptions.{ConnectFailed, ConnectTimeout, ContentTypeException, ReceiveTimeout}
import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.config.SuggestConfig
import com.socrata.soda.server.copy.{Published, Stage}
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName
import com.socrata.thirdparty.metrics.Metrics

case class Suggest(datasetDao: DatasetDAO, columnDao: ColumnDAO,
                   httpClient: HttpClient, config: SuggestConfig) extends Metrics {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  val suggestTimer = metrics.timer("suggest-route")
  val sampleTimer = metrics.timer("suggest-sample-route")

  require(config.port >= 0 && config.port <= 65535, "Port out of range (0 to 65535)")
  val spandexAddress = s"${config.host}:${config.port}"

  val connectTimeoutMS = config.connectTimeout.toMillis.toInt
  require(connectTimeoutMS == config.connectTimeout.toMillis && connectTimeoutMS >= 0,
    "Connect timeout out of range (milliseconds 0 to 2147483647)")

  val receiveTimeoutMS = config.receiveTimeout.toMillis.toInt
  require(receiveTimeoutMS == config.receiveTimeout.toMillis && receiveTimeoutMS >= 0,
    "Receive timeout out of range (milliseconds 0 to 2147483647)")

  def datasetId(resourceName: ResourceName): Option[String] = {
    datasetDao.getDataset(resourceName, None) match {
      case DatasetDAO.Found(d) => Some(d.systemId.underlying)
      case DatasetDAO.NotFound(d) => None
      case x: DatasetDAO.Result =>
        log.error(s"dataset not found $resourceName ${x.getClass.getName}")
        throw new MatchError(x)
    }
  }

  def datacoordinatorColumnId(resourceName: ResourceName, columnName: ColumnName): Option[String] = {
    columnDao.getColumn(resourceName, columnName) match {
      case ColumnDAO.Found(_, c, _) => Some(c.id.underlying)
      case ColumnDAO.ColumnNotFound(c) => None
      case x: ColumnDAO.Result =>
        log.error(s"column not found $columnName ${x.getClass.getName}")
        throw new MatchError(x)
    }
  }

  case class SuggestError(error: Throwable) {
    def json: JValue = {
      JObject(Map(
        "source"       -> JString("soda-fountain"),
        "entity"       -> JString("Suggest"),
        "errorMessage" -> JString(error.toString)
      ))
    }
  }

  // f(dataset name, copy num or lifecycle stage, column name, text) => spandex uri to get
  def go(req: HttpRequest, resp: HttpServletResponse,
         resourceName: ResourceName, columnName: ColumnName, text: String,
         f: (String, Stage, String, String) => URI): Unit = {
    internalContext(resourceName, columnName) match {
      case None => NotFound(resp)
      case Some((ds: String, stage: Stage, col: String)) =>
        val (code: Int, body: JValue) = try {
          getSpandexResponse(f(ds, stage, col, text), req.queryParameters)
        } catch {
          case rt: ReceiveTimeout => log.warn(s"Spandex receive timeout $rt")
            (HttpServletResponse.SC_INTERNAL_SERVER_ERROR, SuggestError(rt).json)
          case ct: ConnectTimeout => log.warn(s"Spandex connect timeout $ct")
            (HttpServletResponse.SC_INTERNAL_SERVER_ERROR, SuggestError(ct).json)
          case cf: ConnectFailed => log.warn(s"Spandex connect failed $cf")
            (HttpServletResponse.SC_INTERNAL_SERVER_ERROR, SuggestError(cf).json)
        }

        (Status(code) ~> Json(body))(resp)
    }
  }

  def internalContext(resourceName: ResourceName, columnName: ColumnName): Option[(String, Stage, String)] = {
    def notFound(name: String) = {
      log.info("{} not found - {}.{}", name, resourceName, columnName)
      None
    }

    for {
      ds <- datasetId(resourceName).orElse(notFound("dataset id"))
      stage <- Some(Published)
      col <- datacoordinatorColumnId(resourceName, columnName).orElse(notFound("column"))
    } yield (ds, stage, col)
  }

  def getSpandexResponse(uri: URI, params: Map[String, String] = Map.empty): (Int, JValue) = {
    val spandexRequest: SimpleHttpRequest = RequestBuilder(uri)
      .connectTimeoutMS(connectTimeoutMS)
      .receiveTimeoutMS(receiveTimeoutMS)
      .addParameters(params)
      .get

    httpClient.execute(spandexRequest).run { spandexResponse =>
      val body = try {
        spandexResponse.jValue()
      } catch {
        case e: ContentTypeException => log.warn(s"Non JSON response: $e")
          JNull
      }
      (spandexResponse.resultCode, body)
    }
  }

  case class sampleService(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def get = { req => resp =>
      sampleTimer.time {
        go(req, resp, resourceName, columnName, "", (dataset, stage, column, _) =>
          new URI(s"http://$spandexAddress/suggest/$dataset/$stage/$column"))
      }
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName, text: String) extends SodaResource {
    override def get = { req => resp =>
      suggestTimer.time {
        go(req, resp, resourceName, columnName, text, (dataset, stage, column, text) => {
          // protect param 'text' from arbitrary url insertion
          val encText = java.net.URLEncoder.encode(text, "utf-8")
          new URI(s"http://$spandexAddress/suggest/$dataset/$stage/$column/$encText")
        })
      }
    }
  }

}

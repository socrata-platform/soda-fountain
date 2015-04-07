package com.socrata.soda.server.resources

import java.net.URI
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.socrata.http.client._
import com.socrata.http.client.exceptions.ContentTypeException
import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.config.SuggestConfig
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName

case class Suggest(datasetDao: DatasetDAO, columnDao: ColumnDAO,
                   httpClient: HttpClient, config: SuggestConfig) {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  if (config.port < 0 || config.port > 65535) throw new IllegalArgumentException("Port out of range (0 to 65535)")
  val spandexAddress = s"${config.host}:${config.port}"

  val connectTimeoutMS = config.connectTimeout.toMillis.toInt
  if (connectTimeoutMS != config.connectTimeout.toMillis || connectTimeoutMS < 0)
    throw new IllegalArgumentException("Connect timeout out of range (milliseconds 0 to 2147483647)")

  val receiveTimeoutMS = config.receiveTimeout.toMillis.toInt
  if (receiveTimeoutMS != config.receiveTimeout.toMillis || receiveTimeoutMS < 0)
    throw new IllegalArgumentException("Receive timeout out of range (milliseconds 0 to 2147483647)")

  def datasetId(resourceName: ResourceName): Option[String] = {
    datasetDao.getDataset(resourceName, None) match {
      case DatasetDAO.Found(d) => Some(d.systemId.underlying)
      case DatasetDAO.NotFound(d) => None
      case x: DatasetDAO.Result =>
        val msg = s"dataset not found $resourceName ${x.getClass.getName}"
        log.error(msg)
        throw new Exception(msg)
    }
  }

  def copyNum(resourceName: ResourceName): Option[Long] =
    datasetDao.getCurrentCopyNum(resourceName)

  def datacoordinatorColumnId(resourceName: ResourceName, columnName: ColumnName): Option[String] = {
    columnDao.getColumn(resourceName, columnName) match {
      case ColumnDAO.Found(_, c, _) => Some(c.id.underlying)
      case ColumnDAO.ColumnNotFound(c) => None
      case x: ColumnDAO.Result =>
        val msg = s"column not found $columnName ${x.getClass.getName}"
        log.error(msg)
        throw new Exception(msg)
    }
  }

  // f(dataset name, copy num, column name, text) => spandex uri to get
  def go(req: HttpRequest, resp: HttpServletResponse,
         resourceName: ResourceName, columnName: ColumnName, text: String,
         f: (String, Long, String, String) => URI): Unit = {
    val (ds, cn, col) = internalContext(resp, resourceName, columnName).get
    val (code, body) = getSpandexResponse(f(ds, cn, col, text), req.queryParameters)
    (Status(code) ~> Json(body))(resp)
  }

  def internalContext(resp: HttpServletResponse,
                      resourceName: ResourceName, columnName: ColumnName): Option[(String, Long, String)] = {
    def notFound(resp: HttpServletResponse, name: String) = {
      NotFound(resp)
      log.info("{} not found - {}.{}", name, resourceName, columnName)
      None
    }

    for {
      ds <- datasetId(resourceName).orElse(notFound(resp, "dataset id"))
      cn <- copyNum(resourceName).orElse(notFound(resp, "copy"))
      col <- datacoordinatorColumnId(resourceName, columnName).orElse(notFound(resp, "column"))
    } yield {
      (ds, cn, col)
    }
  }

  def getSpandexResponse(uri: URI, params: Map[String, String] = Map.empty): (Int, JValue) = {
    log.info(s"GO SPANDEX: $uri")
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
      log.info(s"GET /suggest $resourceName :: $columnName [sample]")
      go(req, resp, resourceName, columnName, "", (dataset, copynum, column, _) =>
        new URI(s"http://$spandexAddress/suggest/$dataset/$copynum/$column"))
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName, text: String) extends SodaResource {
    override def get = { req => resp =>
      log.info(s"GET /suggest $resourceName :: $columnName :: $text")
      go(req, resp, resourceName, columnName, text, (dataset, copynum, column, text) => {
        val encText = java.net.URLEncoder.encode(text, "utf-8") // protect param 'text' from arbitrary url insertion
        new URI(s"http://$spandexAddress/suggest/$dataset/$copynum/$column/$encText")
      })
    }
  }

}

package com.socrata.soda.server.resources

import java.util.concurrent.Executors

import com.rojoma.json.v3.ast.JNull
import com.socrata.http.client.exceptions.ContentTypeException
import com.socrata.http.client.{HttpClientHttpClient, NoopLivenessChecker, RequestBuilder, SimpleHttpRequest}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.config.SuggestConfig
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName

case class Suggest(datasetDao: DatasetDAO, columnDao: ColumnDAO, config: SuggestConfig) {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  lazy val httpClient = new HttpClientHttpClient(Executors.newCachedThreadPool(),
    HttpClientHttpClient.defaultOptions.
      withLivenessChecker(NoopLivenessChecker).
      withUserAgent("soda-fountain-lib"))

  lazy val spandexAddress = s"${config.host}:${config.port}"

  def datasetId(resourceName: ResourceName): String = {
    datasetDao.getDataset(resourceName, None) match {
      case DatasetDAO.Found(d) => d.systemId.underlying
      case DatasetDAO.NotFound(d) => ""// TODO: handle dataset not found
      case x: DatasetDAO.Result =>
        val msg = s"dataset not found $resourceName ${x.getClass.getName}"
        log.error(msg)
        throw new Exception(msg)
    }
  }

  def copyNum(resourceName: ResourceName): Long = {
    datasetDao.getCurrentCopyNum(resourceName).getOrElse({ // TODO: handle copy not found
      val msg = s"dataset copy not found $resourceName"
      log.error(msg)
      throw new Exception(msg)
    })
  }

  def datacoordinatorColumnId(resourceName: ResourceName, columnName: ColumnName): String = {
    columnDao.getColumn(resourceName, columnName) match {
      case ColumnDAO.Found(_, c, _) => c.id.underlying
      case ColumnDAO.ColumnNotFound(c) => "" // TODO: handle column not found
      case x: ColumnDAO.Result =>
        val msg = s"column not found $columnName ${x.getClass.getName}"
        log.error(msg)
        throw new Exception(msg)
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName, text: String) extends SodaResource {
    override def get = { req => resp =>
      val ds = datasetId(resourceName)
      val cn = copyNum(resourceName)
      val col = datacoordinatorColumnId(resourceName, columnName)
      // TODO: protect param 'text' from arbitrary url insertion

      val spandexRequest: SimpleHttpRequest = RequestBuilder(spandexAddress)
        .addPath(s"/suggest/$ds/$cn/$col/$text")
        .get

      for {
        spandexResponse <- httpClient.execute(spandexRequest)
      } yield {
        val body = try {
          spandexResponse.jValue()
        } catch {
          case e: ContentTypeException => log.warn(s"Non JSON response: $e")
            JNull
        }
        (Status(spandexResponse.resultCode) ~> Json(body))(resp)
      }

      // (OK ~> Json(JObject(Map("spandex ok" -> JBoolean(true)))))(resp)
    }
  }

}

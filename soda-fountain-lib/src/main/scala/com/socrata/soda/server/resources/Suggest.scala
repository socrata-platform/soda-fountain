package com.socrata.soda.server.resources

import java.net.URI
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JNull
import com.socrata.http.client._
import com.socrata.http.client.exceptions.ContentTypeException
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.config.SuggestConfig
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName

case class Suggest(datasetDao: DatasetDAO, columnDao: ColumnDAO,
                   httpClient: HttpClient, config: SuggestConfig) {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  lazy val spandexAddress = s"${config.host}:${config.port}"

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

  case class service(resourceName: ResourceName, columnName: ColumnName, text: String) extends SodaResource {

    override def get = { req => resp =>
      log.info(s"GET /suggest $resourceName :: $columnName :: $text")

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
        val encText = java.net.URLEncoder.encode(text, "utf-8") // protect param 'text' from arbitrary url insertion

        val uri = new URI(s"http://$spandexAddress/suggest/$ds/$cn/$col/$encText")
        log.info(s"TRANSLATED $ds|$cn|$col :: $encText")
        log.info(s"SPANDEX GET $uri")
        val spandexRequest: SimpleHttpRequest = RequestBuilder(uri).get

        httpClient.execute(spandexRequest).run { spandexResponse =>
          val body = try {
            spandexResponse.jValue()
          } catch {
            case e: ContentTypeException => log.warn(s"Non JSON response: $e")
              JNull
          }

          (Status(spandexResponse.resultCode) ~> Json(body))(resp)
        }
      }
    }
  }

}

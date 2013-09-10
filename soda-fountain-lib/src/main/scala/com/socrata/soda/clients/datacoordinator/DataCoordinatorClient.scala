package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.util._
import com.rojoma.json.codec._
import com.rojoma.json.ast._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment._
import scala.collection.Map
import com.socrata.http.client._
import com.socrata.http.server.routing.HttpMethods._
import com.rojoma.json.ast.JString
import com.socrata.soda.server.id.{ColumnId, SecondaryId, DatasetId}

object DataCoordinatorClient {

  class JsonInvalidSchemaException(pair: (String, JValue)) extends Exception

  implicit object anotherMapCodec extends JsonCodec[Map[ColumnId, SoQLType]] {
    def encode(x: Map[ColumnId, SoQLType]) = JObject(x.map(pair => (pair._1.underlying, JString(pair._2.name.name))))
    def decode(x: JValue): Option[Map[ColumnId, SoQLType]] = x match {
      case JObject(m) =>
        val map = m.map{ pair =>
          pair._2 match {
            case JString(t) => (new ColumnId(pair._1), SoQLType.typesByName.get(TypeName(t)).getOrElse(throw new JsonInvalidSchemaException(pair)))
            case _ => return None
          }
        }
        Some(map)
      case _ => None
    }
  }

  object SchemaSpec {
    implicit val codec = SimpleJsonCodecBuilder[SchemaSpec].build("hash", _.hash, "pk", _.pk, "schema", _.schema, "locale", _.locale)
  }
  class SchemaSpec(val hash: String, val pk: ColumnId, val schema: Map[ColumnId,SoQLType], val locale: String) {
    override def toString = JsonUtil.renderJson(this)
  }

  object VersionReport{
    implicit val codec = SimpleJsonCodecBuilder[VersionReport].build("version", _.version)
  }
  class VersionReport(val version: Long)


}

trait DataCoordinatorClient {
  import DataCoordinatorClient._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DataCoordinatorClient])

  val internalHttpClient : HttpClient
  def hostO(instance: String): Option[RequestBuilder]
  def createUrl(host: RequestBuilder) = host.p("dataset")
  def mutateUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying)
  def schemaUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying, "schema")
  def secondaryUrl(host: RequestBuilder, secondaryId: SecondaryId, datasetId: DatasetId) = host.p("secondary-manifest", secondaryId.underlying, datasetId.underlying)

  def withHost[T](instance: String)(f: RequestBuilder => T): T =
    hostO(instance) match {
      case Some(host) => f(host)
      case None => throw new Exception("could not connect to data coordinator")
    }

  def withHost[T](datasetId: DatasetId)(f: RequestBuilder => T): T =
    withHost(datasetId.nativeDataCoordinator)(f)

    def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId): Unit =
    withHost(datasetId) { host =>
      val r = secondaryUrl(host, secondaryId, datasetId).method(POST).get
      for (response <- internalHttpClient.execute(r)) yield {
        response.resultCode match {
          case 200 => // ok
          case _ => throw new Exception("could not propagate to secondary")
        }
      }
    }

  def getSchema(datasetId: DatasetId): Option[SchemaSpec] =
    withHost(datasetId) { host =>
      val request = schemaUrl(host, datasetId).get
      for (response <- internalHttpClient.execute(request)) yield {
        if(response.resultCode == 200) {
          val result = response.asValue[SchemaSpec]()
          if(!result.isDefined) throw new Exception("Unable to interpret response as a schemaspec?")
          result
        } else if(response.resultCode == 404) {
          None
        } else {
          throw new Exception("Unexpected result from server: " + response.resultCode)
        }
      }
    }

  // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
  // TODO                                                                  TODO
  // TODO :: ALL THESE NEED TO HANDLE ERRORS FROM THE DATA COORDINATOR! :: TODO
  // TODO                                                                  TODO
  // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

  protected def sendScript[T]( rb: RequestBuilder, script: MutationScript) (f: ((Response) => T)): T = {
    val request = rb.method(POST).json(script.it)
    for (r <- internalHttpClient.execute(request)) yield f(r)
  }

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (DatasetId, Iterable[JValue]) = {
    withHost(instance) { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      sendScript(createUrl(host).method(POST), createScript){ response : Response =>
        log.info("TODO: Handle errors from the data-coordinator")
        val idAndReports = response.asValue[JArray]()
        idAndReports match {
          case Some(JArray(Seq(JString(datasetId), _*))) => (DatasetId(datasetId), idAndReports.get.tail)
          case None => throw new Exception("unexpected response from data coordinator")
        }
      }
    }
  }

  def update[T](datasetId: DatasetId, schema: Option[String], user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Iterator[JValue] => T): T = {
    log.info("TODO: update should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
      sendScript(mutateUrl(host, datasetId).method(POST), updateScript) { r =>
        log.info("TODO: Handle errors from the data-coordinator")
        f(r.asArray[JValue]())
      }
    }
  }

  def copy[T](datasetId: DatasetId, schema: Option[String], copyData: Boolean, user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Iterator[JValue] => T): T = {
    log.info("TODO: copy should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions)
      sendScript(mutateUrl(host, datasetId).method(POST), createScript) { r =>
        log.info("TODO: Handle errors from the data-coordinator")
        f(r.asArray[JValue]())
      }
    }
  }
  def publish[T](datasetId: DatasetId, schema: Option[String], snapshotLimit:Option[Int], user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Iterator[JValue] => T): T = {
    log.info("TODO: publish should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions)
      sendScript(mutateUrl(host, datasetId).method(POST), pubScript) { r =>
        log.info("TODO: Handle errors from the data-coordinator")
        f(r.asArray[JValue]())
      }
    }
  }
  def dropCopy[T](datasetId: DatasetId, schema: Option[String], user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Iterator[JValue] => T): T = {
    log.info("TODO: dropCopy should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val dropScript = new MutationScript(user, DropDataset(schema), instructions)
      sendScript(mutateUrl(host, datasetId).method(POST), dropScript) { r =>
        log.info("TODO: Handle errors from the data-coordinator")
        f(r.asArray[JValue]())
      }
    }
  }
  def deleteAllCopies[T](datasetId: DatasetId, schema: Option[String], user: String)(f: Iterator[JValue] => T): T = {
    log.info("TODO: deleteAllCopies should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val deleteScript = new MutationScript(user, DropDataset(schema), Iterator.empty)
      sendScript(mutateUrl(host, datasetId).method(DELETE), deleteScript) { r =>
        log.info("TODO: Handle errors from the data-coordinator")
        f(r.asArray[JValue]())
      }
    }
  }

  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): VersionReport = {
    withHost(datasetId) { host =>
      val request = secondaryUrl(host, secondaryId, datasetId).get
      for (r <- internalHttpClient.execute(request)) yield {
        log.info("TODO: Handle errors from the data-coordinator")
        val oVer = r.asValue[VersionReport]()
        oVer match {
          case Some(ver) => ver
          case None => throw new Exception("version not found")
        }
      }
    }
  }
}

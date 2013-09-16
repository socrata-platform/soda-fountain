package com.socrata.soda.clients.datacoordinator

import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import com.socrata.soda.server.id.{SecondaryId, DatasetId}
import com.socrata.http.server.routing.HttpMethods
import com.rojoma.json.ast.{JObject, JString, JArray, JValue}
import com.socrata.soda.server.util.schema.SchemaSpec
import javax.servlet.http.HttpServletResponse
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.rojoma.json.codec.JsonCodec

abstract class HttpDataCoordinatorClient(httpClient: HttpClient) extends DataCoordinatorClient {
  import DataCoordinatorClient._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DataCoordinatorClient])

  def hostO(instance: String): Option[RequestBuilder]
  def createUrl(host: RequestBuilder) = host.p("dataset")
  def mutateUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying)
  def schemaUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying, "schema")
  def secondaryUrl(host: RequestBuilder, secondaryId: SecondaryId, datasetId: DatasetId) = host.p("secondary-manifest", secondaryId.underlying, datasetId.underlying)
  def exportUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying)

  def withHost[T](instance: String)(f: RequestBuilder => T): T =
    hostO(instance) match {
      case Some(host) => f(host)
      case None => throw new Exception("could not find data coordinator")
    }

  def withHost[T](datasetId: DatasetId)(f: RequestBuilder => T): T =
    withHost(datasetId.nativeDataCoordinator)(f)

  def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId): Unit =
    withHost(datasetId) { host =>
      val r = secondaryUrl(host, secondaryId, datasetId).method(HttpMethods.POST).get // ick
      for (response <- httpClient.execute(r)) yield {
        response.resultCode match {
          case 200 => // ok
          case _ => throw new Exception("could not propagate to secondary")
        }
      }
    }

  def getSchema(datasetId: DatasetId): Option[SchemaSpec] =
    withHost(datasetId) { host =>
      val request = schemaUrl(host, datasetId).get
      for (response <- httpClient.execute(request)) yield {
        if(response.resultCode == 200) {
          val result = response.asValue[SchemaSpec]()
          if(!result.isDefined) throw new Exception("Unable to interpret data coordinator's response for " + datasetId + " as a schemaspec?")
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

  protected def sendScript[T](rb: RequestBuilder, script: MutationScript)(f: Result => T): T = {
    val request = rb.json(script.it)
    for (r <- httpClient.execute(request)) yield {
      r.resultCode match {
        case HttpServletResponse.SC_OK =>
          f(Success(r.asArray[JValue]()))
        case _ =>
          r.asValue[PossiblyUnknownDataCoordinatorError]().getOrElse(throw new Exception("Response was JSON but not decodable as an error")) match {
            case SchemaMismatch(_, schema) =>
              f(SchemaOutOfDate(schema))
            case UnknownDataCoordinatorError(code, data) =>
              log.error("Unknown data coordinator error " + code)
              log.error("Aux info: " + data)
              throw new Exception("Unknown data coordinator error " + code)
          }
      }
    }
  }

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (DatasetId, Iterable[JValue]) = {
    withHost(instance) { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      sendScript(createUrl(host), createScript) {
        case Success(idAndReports) =>
          val JString(datasetId) = idAndReports.next()
          (DatasetId(datasetId), idAndReports.toSeq)
        case other =>
          throw new Exception("Unexpected response from data-coordinator: " + other)
      }
    }
  }

  def update[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: update should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schemaHash), instructions)
      sendScript(mutateUrl(host, datasetId), updateScript)(f)
    }
  }

  def copy[T](datasetId: DatasetId, schemaHash: String, copyData: Boolean, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: copy should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schemaHash), instructions)
      sendScript(mutateUrl(host, datasetId), createScript)(f)
    }
  }
  def publish[T](datasetId: DatasetId, schemaHash: String, snapshotLimit:Option[Int], user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: publish should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schemaHash), instructions)
      sendScript(mutateUrl(host, datasetId), pubScript)(f)
    }
  }
  def dropCopy[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Result => T): T = {
    log.info("TODO: dropCopy should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val dropScript = new MutationScript(user, DropDataset(schemaHash), instructions)
      sendScript(mutateUrl(host, datasetId), dropScript)(f)
    }
  }

  // Pretty sure this is completely wrong
  def deleteAllCopies[T](datasetId: DatasetId, schemaHash: String, user: String)(f: Result => T): T = {
    log.info("TODO: deleteAllCopies should decode the row op report into something higher-level than JValues")
    withHost(datasetId) { host =>
      val deleteScript = new MutationScript(user, DropDataset(schemaHash), Iterator.empty)
      sendScript(mutateUrl(host, datasetId).method(HttpMethods.DELETE), deleteScript)(f)
    }
  }

  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): VersionReport = {
    withHost(datasetId) { host =>
      val request = secondaryUrl(host, secondaryId, datasetId).get
      for (r <- httpClient.execute(request)) yield {
        log.info("TODO: Handle errors from the data-coordinator")
        val oVer = r.asValue[VersionReport]()
        oVer match {
          case Some(ver) => ver
          case None => throw new Exception("version not found")
        }
      }
    }
  }

  def export[T](datasetId: DatasetId, schemaHash: String)(f: Result => T): T = {
    log.info("TODO TODO TODO send the schema-hash and handle the mismatches")
    withHost(datasetId) { host =>
      val request = exportUrl(host, datasetId).get
      for(r <- httpClient.execute(request)) yield {
        log.info("TODO: Handle errors from the data-coordinator")
        f(Success(r.asArray[JValue]()))
      }
    }
  }
}

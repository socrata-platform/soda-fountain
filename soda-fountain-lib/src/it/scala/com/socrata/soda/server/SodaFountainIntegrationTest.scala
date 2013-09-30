package com.socrata.soda.server

import com.rojoma.json.io._
import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.ast._
import com.socrata.soda.server.config.SodaFountainConfig
import com.typesafe.config.ConfigFactory
import com.socrata.http.client._
import java.util.concurrent.Executors
import scala.Some
import com.rojoma.json.ast.JString
import com.socrata.soda.server.highlevel.DatasetDAO.DatasetVersion
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.VersionReport
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.JsonUtil

trait IntegrationTestHelpers {

  val sodaHost: String = "localhost"
  val secondaryStore = "es"
  val hashHeader = "x-socrata-version-hash"
  val sodaPort = 8080
  val httpClient = new HttpClientHttpClient(NoopLivenessChecker, Executors.newCachedThreadPool(), userAgent = "soda fountain integration test")


  def column(name: String, fieldName: String, oDesc: Option[String], datatype: String): JObject = {
    val base = Map(
      "name" -> JString(name),
      "field_name" -> JString(fieldName),
      "datatype" -> JString(datatype)
    )
    val map = oDesc match { case Some(desc) => base + ("description" -> JString(desc)); case None => base }
    JObject(map)
  }


  def dispatch[B](method: String, pathParts:Seq[String], paramso: Option[Map[String,String]], bodyo: Option[JValue])(f: Response => B) = {
    val req =
      RequestBuilder(sodaHost, false)
        .port(sodaPort)
        .addPaths( pathParts )
        .addParameters( paramso.getOrElse(Map[String,String]()))
        .method(method)
        .addHeader(("Content-type", "application/json"))
    val prepared = bodyo match {
      case Some(jval) => req.json( JValueEventIterator(jval) )
      case None => req.get
    }
    httpClient.execute( prepared ).flatMap(f)
  }

  case class SimpleResponse(val resultCode: Int, val body: JValue)
  def sendWaitRead(method: String, service:String, part1o: Option[String], part2o: Option[String], paramso: Option[Map[String,String]], bodyo: Option[JValue]) = {

    val req =
      RequestBuilder(sodaHost, false)
      .port(sodaPort)
      .addPaths( Seq(Some(service), part1o, part2o).collect{ case Some(part) => part} )
      .addParameters( paramso.getOrElse(Map[String,String]()))
      .method(method)
      .addHeader(("Content-type", "application/json"))
    val prepared = bodyo match {
      case Some(jval) => req.json( JValueEventIterator(jval) )
      case None => req.get
    }
    httpClient.execute( prepared ).flatMap{ response =>
      val body = response.isJson match { case true => response.asJValue(2 ^ 20); case false => JNull}
      SimpleResponse(response.resultCode, body)
    }
  }

  private def requestVersionInSecondaryStore(resourceName: String) = {
    val response = sendWaitRead("GET", "dataset-version", Some(resourceName), Some("es"), None, None)
    response.resultCode match {
      case 200 => JsonCodec[VersionReport].decode(response.body) match {
        case Some(VersionReport(ver)) => Right(ver)
        case None => Left(s"unexpected response for version request: ${response.body}")
      }
      case _ => Left(s"could not read version in secondary store: ${response.toString}")
    }
  }

  def getVersionInSecondaryStore(resourceName: String) : Long = {
    val start = System.currentTimeMillis()
    val limit = 1000
    while ( start + limit > System.currentTimeMillis()) {
      requestVersionInSecondaryStore(resourceName) match {
        case Right(version) => return version
        case Left(err) => Thread.sleep(100)
      }
    }
    throw new Exception(s"${resourceName} not found in secondary store (after waiting ${limit}ms)")
  }

  def waitForSecondaryStoreUpdate(resourceName: String, minVersion: Long = 0): Unit = {
    val start = System.currentTimeMillis()
    val limit = 10000
    while ( start + limit > System.currentTimeMillis()) {
      val currentVersion = getVersionInSecondaryStore(resourceName)
      if (currentVersion > minVersion) return
      Thread.sleep(100)
    }
    throw new Exception(s"timeout while waiting for secondary store to update ${resourceName} past version ${minVersion}")
  }

  def readBody(response: SimpleResponse) = { response.body.toString }
}

object SodaFountainForTest extends SodaFountain(new SodaFountainConfig(ConfigFactory.load())) {

}

trait SodaFountainIntegrationTest extends FunSuite with MustMatchers with IntegrationTestHelpers {

  def jsonCompare(actual:String, expected:String) = {
    val aj = JsonReader.fromString(actual)
    val ej = JsonReader.fromString(expected)
    aj must  equal(ej)
  }
}

package com.socrata.soda.server

import com.rojoma.json.io._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, ParallelTestExecution, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.ast._
import com.socrata.soda.server.config.SodaFountainConfig
import com.typesafe.config.ConfigFactory
import com.socrata.http.client._
import java.util.concurrent.Executors
import scala.Some
import com.rojoma.json.ast.JString
import java.io.{InputStreamReader, BufferedReader}
import scala.io.Source

trait IntegrationTestHelpers {

  val sodaHost: String = "localhost:8080"
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

  def dispatch(method: String, service:String, part1o: Option[String], part2o: Option[String], paramso: Option[Map[String,String]], bodyo: Option[JValue]) = {

    val req = RequestBuilder(sodaHost, false)
    req.addPath(service)
    part1o.foreach(req.addPath _)
    part2o.foreach(req.addPath _)
    paramso.foreach(req.addParameters(_))
    req.method(method)
    httpClient.execute( req.get ).flatMap{ r => r }
    /*
    bodyo match {
      case Some(jval) => httpClient.executeRaw( req.json(jval))
      case None => httpClient.bodylessOp(SimpleHttpRequest(req))
    }
    */
  }

  private def requestVersionInSecondaryStore(resourceName: String) = {
    val response = dispatch("GET", "dataset-version", Some(resourceName), Some("es"), None, None)
    response.resultCode match {
      case 200 => Right(response.asValue[Long](64).get)
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

  def readBody(response: Response) = { Source.fromInputStream(response.asInputStream(2^20)).mkString}

  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

}

object SodaFountainForTest extends SodaFountain(new SodaFountainConfig(ConfigFactory.load())) {

}

trait SodaFountainIntegrationTest extends FunSuite with MustMatchers with IntegrationTestHelpers {

  def jsonCompare(actual:String, expected:String) = {
    normalizeWhitespace(actual) must equal (normalizeWhitespace(expected))
  }
}

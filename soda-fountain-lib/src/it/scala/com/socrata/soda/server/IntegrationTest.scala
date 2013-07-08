package com.socrata.soda.server


import com.rojoma.json.io._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, ParallelTestExecution, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.ast._
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import com.socrata.soda.server.mocks.{LocalDataCoordinator, MockNameAndSchemaStore}
import com.socrata.querycoordinator.client.LocalQueryCoordinatorClient
import com.ning.http.client.RequestBuilder

trait IntegrationTestHelpers {

  val sodaHost: String = "localhost:8080"

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
    val url = host(sodaHost) / service
    val request = part1o.foldLeft(url){ (url1, part1) =>
      part2o.foldLeft( url1 / part1 ) ( (url2, part2) => url2 / part2)
    }
    request.setMethod(method).addHeader("Content-Type", "application/json;charset=utf-8")
    for ( params <- paramso ) yield request <<? params
    for ( body <- bodyo ) yield request.setBody(body.toString)

    val response = for (r <- Http(request).either.right) yield r
    response() match {
      case Right(response) => response
      case Left(thr) => throw thr
    }
  }

  val fountain = new SodaFountain with MockNameAndSchemaStore with LocalDataCoordinator with LocalQueryCoordinatorClient

  private def requestVersionInSecondaryStore(resourceName: String) = {
    val response = dispatch("GET", "dataset-version", Some(resourceName), Some("es"), None, None)
    response.getStatusCode match {
      case 200 => Right(response.getResponseBody.toLong)
      case _ => Left(s"could not read version in secondary store: ${response.getResponseBody}")
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

  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

}

trait IntegrationTest extends FunSuite with MustMatchers with IntegrationTestHelpers {

  def jsonCompare(actual:String, expected:String) = {
    normalizeWhitespace(actual) must equal (normalizeWhitespace(expected))
  }
}

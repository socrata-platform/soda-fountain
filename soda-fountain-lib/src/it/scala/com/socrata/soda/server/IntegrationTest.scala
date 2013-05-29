package com.socrata.soda.server


import com.rojoma.json.io._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, ParallelTestExecution, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.rojoma.json.ast._
import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

object IntegrationTest {

  val hostname: String = "localhost:8080"

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
    val url = host(hostname) / service
    val request = part1o.foldLeft(url){ (url1, part1) =>
      part2o.foldLeft( url1 / part1 ) ( (url2, part2) => url2 / part2)
    }
    request.setMethod(method).addHeader("Content-Type", "application/json;charset=utf-8")
    bodyo match {
      case Some(body) => request.setBody(body.toString)
      case None => request
    }
    val response = for (r <- Http(request).either.right) yield r
    response() match {
      case Right(response) => response
      case Left(thr) => throw thr
    }
  }

  def normalizeWhitespace(fixture: String): String = CompactJsonWriter.toString(JsonReader(fixture).read())

}

class IntegrationTest extends FunSuite with MustMatchers with ParallelTestExecution {

  def jsonCompare(actual:String, expected:String) = {
    IntegrationTest.normalizeWhitespace(actual) must equal (IntegrationTest.normalizeWhitespace(expected))
  }
  def dispatch = IntegrationTest.dispatch _
  def column = IntegrationTest.column _
}

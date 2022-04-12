package com.socrata.querycoordinator

import java.io.{ByteArrayInputStream, InputStream}
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.client.{Response, StandardResponse, _}
import com.socrata.soda.clients.datacoordinator.{ColumnNotFound, HttpDataCoordinatorClient, UnknownDataCoordinatorError}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient.QueryCoordinatorResult
import com.socrata.soda.clients.querycoordinator.{HttpQueryCoordinatorClient, QueryCoordinatorError}
import com.socrata.soda.server.ThreadLimiter
import org.scalatest.{FunSuite, Matchers, Tag}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class HttpQueryCoordinatorClientTest extends FunSuite with Matchers {


  object tag extends Tag("HttpQueryCoordinatorClientTest")

  val client = new MyClient

  // Helper to write errors
  case class QCError(errorCode: String, description: String, data: Map[String, JValue])
  object QCError {
    implicit val jCodec = AutomaticJsonCodecBuilder[QCError]
  }

  case class BadQCError(code: String, description: String, datas: Map[String, JValue])
  object BadQCError {
    implicit val jCodec = AutomaticJsonCodecBuilder[BadQCError]
  }


  test("Unrecognized Error From Data-Coordinator", tag) {
    val status = 400 // could be any 400 or 500 code except
    val myQuery = "my query"
    val data = Map("dataA" -> "A data", "dataB" -> "B data", "dataC" -> "C data")
    val description = "A bad unrecognizable error"
    val code = "update.unknown.error"

    val resp = response(status, code, description, data, true)

    try {
      client.resultFrom(resp, myQuery, new ResourceScope("query client test rs"))
    } catch {
      case e: Exception =>
        e.getMessage should equal(s"Response was JSON but not decodable as an error -  query: $myQuery; code $status")
    }


  }

  test("A recognized Error from Data-Coordinator", tag) {
      val status = 500 // could be any 400 or 500 code except
      val myQuery = "my query"
      val someDataset = "someDataset"
      val someColumn = "someColumn"
      val data = Map("dataset" -> someDataset, "column" -> someColumn)
      val description = "A good recognizable error"
      val code = "update.column.not-found"

      val resp = response(status, code, description, data, false)

      val result = client.resultFrom(resp, myQuery, new ResourceScope("query client test rs"))

      result shouldBe a [QueryCoordinatorResult]

      result match {
        case QueryCoordinatorResult(stat, qce) =>
          stat should equal(status)
          qce shouldBe a [QueryCoordinatorError]
          qce.errorCode should equal(code)
        case _ => fail("did not retrieve query-coordinator error result")
      }
    }

  def response(status: Int, code: String, description: String, data: Map[String, String], isBadResponse: Boolean): Response = {
    if(isBadResponse){
      new StandardResponse(new RInfo(status), badErrorInputStream(code, description, data))
    } else {
      new StandardResponse(new RInfo(status), errorBodyInputStream(code, description, data))
    }

  }


  def errorBodyInputStream(code: String, description: String, data: Map[String, String]): InputStream = {
    val error = QCError(code, description, data.mapValues(JsonEncode.toJValue(_)))
    new ByteArrayInputStream(JsonEncode.toJValue(error).toString().getBytes)
  }

  def badErrorInputStream(code: String, description: String, data: Map[String, String]): InputStream = {
    val error = BadQCError(code, description, data.mapValues(JsonEncode.toJValue(_)))
    new ByteArrayInputStream(JsonEncode.toJValue(error).toString().getBytes)
  }


  class RInfo(code: Int) extends ResponseInfo {

    def resultCode = code
    def headers(name: String): Array[String] = Seq("application/json").toArray
    def headerNames = Set.empty
  }

  class MyClient extends HttpQueryCoordinatorClient{
    val threadLimiter = new ThreadLimiter("TestClient", 50)
    val httpClient: HttpClient = null
    val defaultQueryTimeout = Duration(1, TimeUnit.MINUTES)

    def qchost: Option[RequestBuilder] = None
  }

}

package com.socrata.datacoordinator.client

import java.io.ByteArrayInputStream
import java.io.InputStream

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.http.client._
import com.socrata.http.client.Response
import com.socrata.http.client.StandardResponse
import com.socrata.soda.clients.datacoordinator.{ColumnNotFound, UnknownDataCoordinatorError, HttpDataCoordinatorClient}

import org.scalatest.{FunSuite, Matchers, Tag}

class HttpDatatCoordinatorClientTest extends FunSuite with Matchers {


  object tag extends Tag("HttpDatatCoordinatorClientTest")

  val client = new MyClient

  // Helper to write errors
  case class ReqError(errorCode: String, data: Map[String, JValue])
  object ReqError{
    implicit val jCodec = AutomaticJsonCodecBuilder[ReqError]
  }



  test("Unrecognized Error From Data-Coordinator", tag) {
    val status = 400 // could be any 400 or 500 code except
    val data = Map("dataA" -> JString("A data"), "dataB" -> JString("B data"))
    val code = "update.unknown.error"

    val res = response(status, code, data)

    client.errorFrom(res) match {
      case Some(x) =>{
        x shouldBe a [UnknownDataCoordinatorError]

        x match {
          case y: UnknownDataCoordinatorError =>
            y.errorCode should equal("update.unknown.error")
          case _ => fail("could not match to UnknownDataCoordinatorError")
        }

      }
      case None => fail("should have obtained an error")
    }
  }

  test("A recognized Error from Data-Coordinator", tag) {
      val status = 404 // could be any 400 or 500 code except
      val someDataset = "someDataset"
      val someColumn = "someColumn"
      val someCommandIndex = 40L
      val data = Map("dataset" -> JString(someDataset), "column" -> JString(someColumn), "commandIndex" -> JNumber(someCommandIndex))
      val code = "update.column.not-found"

      val res = response(status, code, data)

      client.errorFrom(res) match {
        case Some(x) =>{
          x shouldBe a [ColumnNotFound]

          x match {
            case ColumnNotFound(dataset, column, commandIndex) =>
              dataset.underlying should equal(someDataset)
              column.underlying should equal(someColumn)
              commandIndex.underlying should equal(someCommandIndex)
            case _ => fail("could not match to UnknownDataCoordinatorError")
          }

        }
        case None => fail("should have obtained an error")
      }


    }

  def response(status: Int, code: String, data: Map[String, JValue]): Response = {
    new StandardResponse(new RInfo(status), errorBodyInputStream(code, data))
  }


  def errorBodyInputStream(code: String, data: Map[String, JValue]): InputStream = {
    val error = ReqError(code, data.mapValues(JsonEncode.toJValue(_)))
    new ByteArrayInputStream(JsonEncode.toJValue(error).toString().getBytes)
  }


  class RInfo(code: Int) extends ResponseInfo {

    def resultCode = code
    def headers(name: String): Array[String] = Seq("application/json").toArray
    def headerNames = Set.empty
  }

  class MyClient extends HttpDataCoordinatorClient(null){
    def hostO(instance: String): Option[RequestBuilder] = None

  }

}

package com.socrata.soda.server.services

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.socrata.soda.server.services.ClientRequestExtractor._
import com.rojoma.json.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.json.io.{JsonBadParse, JsonReader}
import scala.collection.Map
import scala.concurrent.ExecutionContext.Implicits.global
import com.socrata.http.server.responses


trait DatasetService extends SodaService {

  object dataset {

    val MAX_DATUM_SIZE = SodaService.config.getInt("max-dataum-size")

    def upsert(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val it = streamJsonArrayValues(request, MAX_DATUM_SIZE)
      it match {
        case Right(boundedIt) => {
          try {
            withDatasetId(resourceName){ datasetId =>
              val sf = dc.getSchema(datasetId)
              sf() match {
                case Right(schema) => {
                  val upserts = boundedIt.map { rowjval =>
                    rowjval match {
                      case JObject(map) =>  UpsertRow(map)
                      case _ => throw new Error("unexpected value")
                    }
                  }
                  val schema = Option(request.getParameter("schema"))
                  val r = dc.update(datasetId, schema, mockUser, upserts)
                  r() match {
                    case Right(rr) => {
                      DataCoordinatorClient.passThroughResponse(rr)
                    }
                    case Left(thr) => sendErrorResponse(thr.getMessage, "upsert.error", InternalServerError, None)
                  }
                }
                case Left(err) => sendErrorResponse(err, "schema.not-found", NotFound, None)
              }
            }
          }
          catch {
            case bp: JsonBadParse => sendErrorResponse("bad JSON value: " + bp.getMessage, "json.value.invalid", BadRequest, None)
          }
        }
        case Left(err) => sendErrorResponse("Error upserting values", err, BadRequest, None)
      }
    }

    def replace(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def truncate(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def query(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      withDatasetId(resourceName) { datasetId =>
        val q = Option(request.getParameter("$query")).getOrElse("select *")
        val r = qc.query(datasetId, q)
        r() match {
          case Right(response) => {
            responses.Status(response.getStatusCode) ~>  ContentType(response.getContentType) ~> Content(response.getResponseBody)
          }
          case Left(err) => sendErrorResponse(err.getMessage, "internal.error", InternalServerError, None)
        }
      }
    }

   def create()(request:HttpServletRequest): HttpServletResponse => Unit =  {
      DatasetSpec(request.getReader) match {
        case Right(dspec) => {
          val columnInstructions = dspec.columns.map(c => new AddColumnInstruction(c.fieldName, c.dataType))
          val instructions = dspec.rowId match{
            case Some(rid) => columnInstructions :+ SetRowIdColumnInstruction(rid)
            case None => columnInstructions
          }
          val rnf = store.translateResourceName(dspec.resourceName)
          rnf() match {
            case Left(err) => {
              val r = dc.create(dspec.resourceName, mockUser, Some(instructions.iterator), dspec.locale )
              r() match {
                case Right((datasetId, records)) => {
                  store.add(dspec.resourceName, datasetId)  // TODO: handle failure here, see list of errors from DC.
                  dc.propagateToSecondary(datasetId)
                  OK
                }
                case Left(thr) => sendErrorResponse("could not create dataset", "internal.error", InternalServerError, Some(JString(thr.getMessage)))
              }
            }
            case Right(datasetId) => sendErrorResponse("Dataset already exists", "dataset.already.exists", BadRequest, None)
          }
        }
        case Left(ers: Seq[String]) => sendErrorResponse( ers.mkString(" "), "dataset.specification.invalid", BadRequest, None )
      }
    }
    def setSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ColumnSpec.array(request.getReader) match {
        case Right(specs) => ???
        case Left(ers) => sendErrorResponse( ers.mkString(" "), "column.specification.invalid", BadRequest, None )
      }
    }
    def getSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      withDatasetId(resourceName){ id =>
        val f = dc.getSchema(id)
        val r = f()
        r match {
          case Right(schema) => OK ~> Content(schema.toString)
          case Left(err) => sendErrorResponse(err, "dataset.schema.notfound", NotFound, Some(JString(resourceName)))
        }
      }
    }
    def delete(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      withDatasetId(resourceName){ datasetId =>
        val schema = Option(request.getParameter("schema"))
        val d = dc.deleteAllCopies(datasetId, schema, mockUser)
        d() match {
          case Right(response) => {
            store.remove(resourceName)
            DataCoordinatorClient.passThroughResponse(response)
          }
          case Left(thr) => sendErrorResponse(thr.getMessage, "failed.delete", InternalServerError, None)
        }
      }
    }

    def copy(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      withDatasetId(resourceName){ datasetId =>
        val doCopyData = Option(request.getParameter("copy_data")).getOrElse("false").toBoolean
        val schema = Option(request.getParameter("schema"))
        val c = dc.copy(datasetId, schema, doCopyData, mockUser, None)
        c() match {
          case Right(response) => DataCoordinatorClient.passThroughResponse(response)
          case Left(thr) => sendErrorResponse(thr.getMessage, "failed.copy", InternalServerError, None)
        }
      }
    }

    def dropCopy(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      withDatasetId(resourceName){ datasetId =>
        val schema = Option(request.getParameter("schema"))
        val d = dc.dropCopy(datasetId, schema, mockUser, None)
        d() match {
          case Right(response) => DataCoordinatorClient.passThroughResponse(response)
          case Left(thr) => sendErrorResponse(thr.getMessage, "failed.drop", InternalServerError, None)
        }
      }
    }

    def publish(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      withDatasetId(resourceName){ datasetId =>
        val schema = Option(request.getParameter("schema"))
        val snapshowLimit = Option(request.getParameter("snapshot_limit")).flatMap( s => Some(s.toInt) )
        val p = dc.publish(datasetId, schema, snapshowLimit, mockUser, None)
        p() match {
          case Right(response) => DataCoordinatorClient.passThroughResponse(response)
          case Left(thr) => sendErrorResponse(thr.getMessage, "failed.publish", InternalServerError, None)
        }
      }
    }


  }
}

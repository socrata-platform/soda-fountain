package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.socrata.soda.server.services.ClientRequestExtractor._
import com.rojoma.json.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.json.io.{JsonReaderException, JsonBadParse, JsonReader}
import com.socrata.http.server.responses
import com.socrata.soda.server.types.TypeChecker
import com.socrata.datacoordinator.client.DataCoordinatorClient.SchemaSpec


trait DatasetService extends SodaService {

  object dataset {

    val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetService])
    val MAX_DATUM_SIZE = SodaService.config.getInt("max-dataum-size")

    protected def prepareForUpsert(resourceName: String, request: HttpServletRequest)(f: (String, SchemaSpec, Iterator[RowUpdate]) => HttpServletResponse => Unit) : HttpServletResponse => Unit = {
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      val it = streamJsonArrayValues(request, MAX_DATUM_SIZE)
      it match {
        case Right(boundedIt) => {
          try {
            withDatasetId(resourceName){ datasetId =>
              withDatasetSchema(datasetId) { schema =>
                val upserts = boundedIt.map { rowjval =>
                  rowjval match {
                    case JObject(map) =>  {
                      map.foreach{ pair =>
                        val expectedTypeName = schema.schema.get(pair._1).getOrElse( return sendErrorResponse("no column " + pair._1, "dataset.prepareForUpsert.upsert.column.not.found", BadRequest, Some(rowjval), "JSON.row.mapping", resourceName, datasetId))
                        TypeChecker.check(expectedTypeName, pair._2) match {
                          case Right(v) => v
                          case Left(msg) => return sendErrorResponse(msg, "dataset.prepareForUpsert.upsert.type.error", BadRequest, Some(rowjval), "type.checking", resourceName, datasetId)
                        }
                      }
                      UpsertRow(map)
                    }
                    case JArray(Seq(id)) => {
                      val idString = id match {
                        case JNumber(num) => num.toString
                        case JString(str) => str
                        case _ => return sendErrorResponse("row ID for delete operation must be number or string", "dataset.prepareForUpsert.identifier.notNumberOrString", BadRequest, Some(id), resourceName, datasetId)
                      }
                      val pk = pkValue(idString, schema)
                      DeleteRow(pk)
                    }
                    case _ => return sendErrorResponse("could not deserialize into JSON row operation", "dataset.prepareForUpsert.json.row.object.notvalid", BadRequest, Some(rowjval), resourceName, datasetId)
                  }
                }
                f(datasetId, schema, upserts)
              }
            }
          }
          catch {
            case bp: JsonReaderException => sendErrorResponse("invalid JSON: " + bp.getMessage, "dataset.prepareForUpsert.json.invalid", BadRequest, None, resourceName)
          }
        }
        case Left(err) => sendErrorResponse("Error reading JSON: " + err, "dataset.prepareForUpsert.json.iterator.error", BadRequest, None, resourceName)
      }
    }

    def upsert(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      prepareForUpsert(resourceName, request){ (datasetId, schema, upserts) =>
        val r = dc.update(datasetId, schemaHash(request), mockUser, upserts)
        passThroughResponse(r, start, "dataset.upsert", resourceName, datasetId)
      }
    }

    def replace(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      prepareForUpsert(resourceName, request){ (datasetId, schema, upserts) =>
        val instructions: Iterator[DataCoordinatorInstruction] = Iterator.single(RowUpdateOptionChange(truncate = true)) ++ upserts
        val r = dc.update(datasetId, schemaHash(request), mockUser, instructions)
        passThroughResponse(r, start, "dataset.replace", resourceName, datasetId)
      }
    }

    def truncate(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName) { datasetId =>
        val c = dc.update(datasetId, schemaHash(request), mockUser, Array(RowUpdateOptionChange(truncate = true)).iterator)
        passThroughResponse(c, start, "dataset.truncate", resourceName, datasetId)
      }
    }

    def query(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName) { datasetId =>
        val q = Option(request.getParameter("$query")).getOrElse("select *")
        val r = qc.query(datasetId, q)
        r() match {
          case Right(response) => {
            val r = responses.Status(response.getStatusCode) ~>  ContentType(response.getContentType) ~> Content(response.getResponseBody)
            log.info(s"query.response: ${resourceName} ${datasetId} ${q} took ${System.currentTimeMillis - start}ms returned ${response.getStatusText} - ${response.getStatusCode} ")
            r
          }
          case Left(err) => sendErrorResponse(err.getMessage, "dataset.query.internal.error", InternalServerError, None, resourceName, datasetId, q)
        }
      }
    }

  def create()(request:HttpServletRequest): HttpServletResponse => Unit =  {
     val start = System.currentTimeMillis
     DatasetSpec(request.getReader) match {
      case Right(dspec) => {
        if (!validName(dspec.resourceName)) { return sendInvalidNameError(dspec.resourceName, request)}
        val columnInstructions = dspec.columns.map(c => new AddColumnInstruction(c.fieldName, c.dataType))
        val instructions = dspec.rowId match{
          case Some(rid) => columnInstructions :+ SetRowIdColumnInstruction(rid)
          case None => columnInstructions
        }
        val rnf = store.translateResourceName(dspec.resourceName)
        rnf() match {
          case Left(err) => { //TODO: can I be more specific here?
            val r = dc.create(mockUser, Some(instructions.iterator), dspec.locale )
            r() match {
              case Right((datasetId, records)) => {
                val f = store.add(dspec.resourceName, datasetId)  // TODO: handle failure here, see list of errors from DC.
                f()
                log.info(s"create.response ${dspec.resourceName} as ${datasetId} took ${System.currentTimeMillis - start}ms created OK - 200")
                OK
              }
              case Left(thr) => sendErrorResponse("could not create dataset", "dataset.create.internal.error", InternalServerError, Some(JString(thr.getMessage)), dspec.resourceName)
            }
          }
          case Right(datasetId) => sendErrorResponse("Dataset already exists", "dataset.create.resourceName.conflict", BadRequest, None, dspec.resourceName, datasetId)
        }
      }
      case Left(ers: Seq[String]) => sendErrorResponse( ers.mkString(" "), "dataset.create.specification.invalid", BadRequest, None )
     }
  }
    def setSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      ColumnSpec.array(request.getReader) match {
        case Right(specs) => ???
        case Left(ers) => sendErrorResponse( ers.mkString(" "), "dataset.setSchema.column.specification.invalid", BadRequest, None, resourceName )
      }
    }
    def getSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ id =>
        val f = dc.getSchema(id)
        val r = f()
        r match {
          case Right(schema) =>
            log.info(s"getSchema for ${resourceName} ${id} took ${System.currentTimeMillis - start} OK - 200")
            OK ~> Content(schema.toString)
          case Left(err) => sendErrorResponse(err, "dataset.getSchema.notfound", NotFound, Some(JString(resourceName)), resourceName)
        }
      }
    }
    def delete(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        val d = dc.deleteAllCopies(datasetId, schemaHash(request), mockUser)
        d() match {
          case Right(response) => {
            store.remove(resourceName) //TODO: handle error case!
            passThroughResponse(response, start, "dataset.delete", resourceName, datasetId)
          }
          case Left(thr) => sendErrorResponse(thr.getMessage, "dataset.delete.internal.error", InternalServerError, None, resourceName, datasetId)
        }
      }
    }

    def copy(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        val doCopyData = Option(request.getParameter("copy_data")).getOrElse("false").toBoolean
        val c = dc.copy(datasetId, schemaHash(request), doCopyData, mockUser, None)
        passThroughResponse(c, start, "dataset.copy", resourceName, datasetId)
      }
    }

    def dropCopy(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        val d = dc.dropCopy(datasetId, schemaHash(request), mockUser, None)
        passThroughResponse(d, start, "dataset.dropCopy", resourceName, datasetId)
      }
    }

    def publish(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        val snapshowLimit = Option(request.getParameter("snapshot_limit")).flatMap( s => Some(s.toInt) )
        val p = dc.publish(datasetId, schemaHash(request), snapshowLimit, mockUser, None)
        p() match {
          case Right(response) => {
            dc.propagateToSecondary(datasetId)
            passThroughResponse(response, start, "dataset.publish", resourceName, datasetId)
          }
          case Left(thr) => sendErrorResponse(thr.getMessage, "dataset.publish.internal.error", InternalServerError, None, resourceName, datasetId)
        }
      }
    }

    def checkVersionInSecondary(resourceName: String, secondaryName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName) { datasetId =>
        val response = dc.checkVersionInSecondary(datasetId, secondaryName)
        response match {
          case Right(response) => OK ~> Content(response.version.toString)
          case Left(err) => InternalServerError ~> Content(err)
        }
      }
    }
  }
}

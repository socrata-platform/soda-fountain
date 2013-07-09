package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.socrata.soda.server.services.ClientRequestExtractor._
import com.socrata.http.server.responses


trait DatasetService extends SodaService {

  object dataset {

    val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetService])

    def upsert(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      jsonArrayValuesStream(request, MAX_DATUM_SIZE) match {
        case Right(boundedIt) =>
          prepareForUpsert(resourceName, boundedIt, "dataset.upsert"){ (datasetId, schema, upserts) =>
            val r = dc.update(datasetId, schemaHash(request), mockUser, upserts)
            passThroughResponse(r, start, "dataset.upsert", resourceName, datasetId)
          }
        case Left(err) => return sendErrorResponse("Error reading JSON: " + err, "dataset.upsert.json.iterator.error", BadRequest, None, resourceName)
      }
    }

    def replace(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      jsonArrayValuesStream(request, MAX_DATUM_SIZE) match {
        case Right(boundedIt) =>
          prepareForUpsert(resourceName, boundedIt, "dataset.replace"){ (datasetId, schema, upserts) =>
            val instructions: Iterator[DataCoordinatorInstruction] = Iterator.single(RowUpdateOptionChange(truncate = true)) ++ upserts
            val r = dc.update(datasetId, schemaHash(request), mockUser, instructions)
            passThroughResponse(r, start, "dataset.replace", resourceName, datasetId)
          }
        case Left(err) => sendErrorResponse("Error reading JSON: " + err, "dataset.replace.json.iterator.error", BadRequest, None, resourceName)
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
          case Left(err) => sendErrorResponse(err, "internal error during dataset query", "dataset.query.internal.error", InternalServerError, None, resourceName, datasetId, q)
        }
      }
    }

  def create()(request:HttpServletRequest): HttpServletResponse => Unit =  {
     val start = System.currentTimeMillis
     DatasetSpec(request, MAX_DATUM_SIZE) match {
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
              case Left(thr) => sendErrorResponse(thr, "internal error during dataset creation", "dataset.create.internal.error", InternalServerError, Some(Map(("resource_name" -> JString(dspec.resourceName)))), dspec.resourceName)
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
      ColumnSpec.array(request, MAX_DATUM_SIZE) match {
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
          case Left(err) => sendErrorResponse(err, "internal error requesting dataset schema", "dataset.getSchema.not-found", NotFound, Some(Map(("resource_name" -> JString(resourceName)))), resourceName)
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
          case Left(thr) => sendErrorResponse(thr, "internal error during dataset delete", "dataset.delete.internal.error", InternalServerError, None, resourceName, datasetId)
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
          case Left(thr) => sendErrorResponse(thr, "internal error during dataset publish", "dataset.publish.internal.error", InternalServerError, None, resourceName, datasetId)
        }
      }
    }

    def checkVersionInSecondary(resourceName: String, secondaryName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName) { datasetId =>
        val response = dc.checkVersionInSecondary(datasetId, secondaryName)
        response() match {
          case Right(report) =>
            OK ~> Content(report.version.toString)
          case Left(err) =>
            InternalServerError ~> Content(err.getMessage)
        }
      }
    }
  }
}

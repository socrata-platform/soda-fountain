package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.RequestId.{RequestId, ReqIdHeader}
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.copy.Latest
import com.socrata.soda.server.highlevel.ColumnDAO.Result
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.wiremodels.UserProvidedColumnSpec
import com.socrata.soda.server.SodaUtils
import com.socrata.soql.environment.ColumnName
import scala.util.control.ControlThrowable

// TODO: This shouldn't be referenced here.
import com.socrata.http.server.util.{NoPrecondition, Precondition}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord, NameAndSchemaStore}

class ColumnDAOImpl(dc: DataCoordinatorClient,
                    fbm: FeedbackSecondaryManifestClient,
                    store: NameAndSchemaStore,
                    columnSpecUtils: ColumnSpecUtils) extends ColumnDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnDAOImpl])

  def replaceOrCreateColumn(user: String,
                            dataset: ResourceName,
                            precondition: Precondition,
                            expectedDataVersion: Option[Long],
                            column: ColumnName,
                            rawSpec: UserProvidedColumnSpec,
                            requestId: RequestId): ColumnDAO.Result = {
    // TODO: This really needs to be a transaction.  It WILL FAIL if a dataset frequently read is being updated, because one of the readers will have generated dummy columns as part of inconsistency resolution
    val newFieldName = rawSpec.fieldName.getOrElse(column)
    val spec = rawSpec.copy(fieldName = Some(newFieldName))
    store.lookupDataset(dataset, Some(Latest)) match {
      case Some(datasetRecord) =>
        datasetRecord.columnsByName.get(column) match {
          case Some(columnRecord) =>
            doUpdateColumn(datasetRecord, store.latestCopyNumber(dataset), columnRecord, user, precondition, expectedDataVersion, column, spec, requestId)
          case None =>
            createColumn(user, datasetRecord, precondition, expectedDataVersion, column, spec, requestId)
        }
      case None =>
        ColumnDAO.DatasetNotFound(dataset)
    }
  }

  def createColumn(user: String,
                   datasetRecord: DatasetRecord,
                   precondition: Precondition,
                   expectedDataVersion: Option[Long],
                   column: ColumnName,
                   userProvidedSpec: UserProvidedColumnSpec,
                   requestId: RequestId): ColumnDAO.Result = {
    columnSpecUtils.freezeForCreation(datasetRecord.columnsByName.mapValues(col => (col.id, col.typ)), userProvidedSpec) match {
      case ColumnSpecUtils.Success(spec) =>
        if(spec.fieldName != column) return ColumnDAO.InvalidColumnName(column)

        precondition.check(None, sideEffectFree = true) match {
          case Precondition.Passed =>
            val extraHeaders = SodaUtils.traceHeaders(requestId, datasetRecord.resourceName)
            val addColumn = AddColumnInstruction(spec.datatype, spec.fieldName, Some(spec.id), spec.computationStrategy)
            dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user,
                      Iterator.single(addColumn)) {
              case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, lastModified) =>
                // TODO: This next line can fail if a reader has come by and noticed the new column between the dc.update and here
                store.addColumn(datasetRecord.systemId, copyNumber, spec)
                store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                log.info("column created {} {} {}", datasetRecord.systemId.toString, copyNumber.toString, column.name)
                spec.computationStrategy.foreach { strategy =>
                  fbm.maybeReplicate(datasetRecord.handle, Set(strategy.strategyType))
                }
                ColumnDAO.Created(spec.asRecord, etag)
              case DataCoordinatorClient.ColumnExistsAlreadyResult(datasetId, columnId, _) =>
                ColumnDAO.ColumnAlreadyExists(column)
              case DataCoordinatorClient.IllegalColumnIdResult(_, _) =>
                ColumnDAO.IllegalColumnId(column)
              case DataCoordinatorClient.InvalidSystemColumnOperationResult(_, _, _) =>
                ColumnDAO.InvalidSystemColumnOperation(column)
              case DataCoordinatorClient.ColumnNotFoundResult(_, _, _) =>
                ColumnDAO.ColumnNotFound(column)
              case DataCoordinatorClient.DuplicateValuesInColumnResult(_, columnId, _) =>
                 ColumnDAO.DuplicateValuesInColumn(spec.asRecord)
              case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                ColumnDAO.DatasetNotFound(datasetRecord.resourceName)
              case DataCoordinatorClient.DatasetVersionMismatchResult(_, version) =>
                ColumnDAO.DatasetVersionMismatch(datasetRecord.resourceName, version)
              case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                ColumnDAO.InternalServerError(code, tag, data)
              case x =>
                log.warn("case is NOT implemented %s".format(x.toString))
                ColumnDAO.InternalServerError("unknown", tag, x.toString)
            }
          case f: Precondition.Failure =>
            ColumnDAO.PreconditionFailed(f)
        }
      // TODO other cases have not been implemented
      case err@x =>
        log.warn(s"case is NOT implemented ${err.getClass.getName}")
        ???

    }
  }

  class Retry extends ControlThrowable

  def retryable[T](limit: Int /* does not include the initial try */)(f: => T): T = {
    var count = 0
    var done = false
    var result: T = null.asInstanceOf[T]
    do {
      try {
        result = f
        done = true
      } catch {
        case _: Retry =>
          count += 1
          if(count > limit) throw new Exception("Retried too many times")
      }
    } while(!done)
    result
  }
  def retry() = throw new Retry

  def makePK(user: String, resource: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, requestId: RequestId): Result = {
    retryable(limit = 5) {
      store.lookupDataset(resource, Some(Latest)) match {
        case Some(datasetRecord) =>
          datasetRecord.columnsByName.get(column) match {
            case Some(columnRecord) =>
              if(datasetRecord.primaryKey == columnRecord.id) {
                ColumnDAO.Updated(columnRecord, None)
              } else {
                val instructions =
                  if(datasetRecord.primaryKey == ColumnId(":id")) {
                    List(SetRowIdColumnInstruction(columnRecord.id))
                  } else if(columnRecord.id == ColumnId(":id")) {
                    List(DropRowIdColumnInstruction(datasetRecord.primaryKey))
                  } else {
                    List(
                      DropRowIdColumnInstruction(datasetRecord.primaryKey),
                      SetRowIdColumnInstruction(columnRecord.id))
                  }
                val extraHeaders = SodaUtils.traceHeaders(requestId, resource)
                dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user, instructions.iterator) {
                  case DataCoordinatorClient.NonCreateScriptResult(_, _, copyNumber, newVersion, lastModified) =>
                    store.setPrimaryKey(datasetRecord.systemId, columnRecord.id, copyNumber)
                    store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                    ColumnDAO.Updated(columnRecord, None)
                  case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
                    store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
                    retry()
                  case DataCoordinatorClient.DuplicateValuesInColumnResult(_, _, _) =>
                    ColumnDAO.DuplicateValuesInColumn(columnRecord)
                  case DataCoordinatorClient.ColumnExistsAlreadyResult(_, _, _) =>
                    ColumnDAO.ColumnAlreadyExists(column)
                  case DataCoordinatorClient.IllegalColumnIdResult(_, _) =>
                    ColumnDAO.IllegalColumnId(column)
                  case DataCoordinatorClient.InvalidSystemColumnOperationResult(_, _, _) =>
                    ColumnDAO.InvalidSystemColumnOperation(column)
                  case DataCoordinatorClient.ColumnNotFoundResult(_, _, _) =>
                    ColumnDAO.ColumnNotFound(column)
                  case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                    ColumnDAO.DatasetNotFound(resource)
                  case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                    ColumnDAO.InternalServerError(code, tag, data)
                  case x =>
                    log.warn("case is NOT implemented %s".format(x.toString))
                    ColumnDAO.InternalServerError("unknown", tag, x.toString)
                }
              }
            case None =>
              ColumnDAO.ColumnNotFound(column)
          }
        case None =>
          ColumnDAO.DatasetNotFound(resource)
      }
    }
  }

  def updateColumn(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, spec: UserProvidedColumnSpec, requestId: RequestId): Result = {
    retryable(limit = 3) {
      spec match {
        case UserProvidedColumnSpec(None, fieldName, datatype, None, _) =>
          val copyNumber = store.latestCopyNumber(dataset)
          store.lookupDataset(dataset, copyNumber) match {
            case Some(datasetRecord) =>
              datasetRecord.columnsByName.get(column) match {
                case Some(columnRef) =>
                  doUpdateColumn(datasetRecord, copyNumber, columnRef, user, NoPrecondition, expectedDataVersion, column, spec, requestId)
//                  if (datatype.exists (_ != columnRef.typ)) {
//                    // TODO: Allow some datatype conversions?
//                    throw new Exception("Does not support changing datatype.")
//                  } else {
//                    store.updateColumnFieldName(datasetRecord.systemId, columnRef.id, spec.fieldName.get, copyNumber) match {
//                      case 1 =>
//                        val updatedColumnRef = columnRef.copy(fieldName = spec.fieldName.get)
//                        ColumnDAO.Updated(updatedColumnRef, None)
//                      case n =>
//                        throw new Exception("Expect 1 from update single column, got $n")
//                    }
//                  }
                case None =>
                  ColumnDAO.ColumnNotFound(column)
              }
            case None =>
              ColumnDAO.DatasetNotFound(dataset)
          }
        case _ =>
          throw new Exception("Update column get an unsupported column spec.")
      }
    }
  }

  def doUpdateColumn(datasetRecord: DatasetRecord, copyNumber: Long, columnRecord: ColumnRecord, user: String, precondition: Precondition, expectedDataVersion: Option[Long], column: ColumnName, spec: UserProvidedColumnSpec, requestId: RequestId): Result = {
    // ok.  Bleh.  We have a thing and a thing, and we need to decide what's changed.
    // we need to prevent "fieldName" from colliding with existing names.

    // All these errors should really be generated by data-coordinator and just get
    // plumbed through from the mutation script...
    spec.fieldName.foreach { newFieldName =>
      if(datasetRecord.columnsByName.filterNot(_._2.fieldName == columnRecord.fieldName).contains(newFieldName)) return ColumnDAO.ColumnAlreadyExists(newFieldName)
    }
    spec.datatype.foreach { newDatatype =>
      if(newDatatype != columnRecord.typ) return ColumnDAO.CannotChangeColumnType
    }
    spec.id.foreach { newId =>
      if(newId != columnRecord.id) return ColumnDAO.CannotChangeColumnId
    }

    store.withColumnUpdater(datasetRecord.systemId, copyNumber, columnRecord.id) { updater =>
      val instructionsBuilder = List.newBuilder[DataCoordinatorInstruction]
      spec.fieldName.foreach { fn =>
        if (columnRecord.fieldName != fn) {
          updater.updateFieldName(fn)
          instructionsBuilder += SetFieldNameInstruction(columnRecord.id, fn)
        }
      }

      columnSpecUtils.freezeForCreation(datasetRecord.columnsByName.mapValues(col => (col.id, col.typ)), spec, forUpdate = true) match {
        case ColumnSpecUtils.Success(spec) =>
          spec.computationStrategy.foreach { cs =>
            if (columnRecord.computationStrategy.isEmpty) {
              store.addComputationStrategy(datasetRecord.systemId, copyNumber, spec.copy(id = columnRecord.id))
              instructionsBuilder += AddComputationStrategyInstruction(columnRecord.id, cs)
            }
          }
          if (spec.computationStrategy.isEmpty && columnRecord.computationStrategy.nonEmpty) {
            store.dropComputationStrategy(datasetRecord.systemId, copyNumber, spec.copy(id = columnRecord.id))
            instructionsBuilder += DropComputationStrategyInstruction(columnRecord.id)
          }
        case x =>
          log.warn("columnSpecUtils.freezeForCreate(update) failed and ignored", x)
      }

      val instructions = instructionsBuilder.result()

      retryable(limit = 3) {
        dc.update(datasetRecord.handle,
          datasetRecord.schemaHash,
          expectedDataVersion,
          user,
          instructions.iterator) {
          case DataCoordinatorClient.NonCreateScriptResult(_, etag, copyNumber, newVersion, lastModified) =>
            val updatedColumnRec = columnRecord.copy(fieldName = spec.fieldName.getOrElse(columnRecord.fieldName))

            // maybe add secondary manifest
            val newComputationStrategies = instructions.collect { case x: AddComputationStrategyInstruction => x }
            newComputationStrategies.foreach { strategy =>
              fbm.maybeReplicate(datasetRecord.handle, Set(strategy.strategy.strategyType))
            }

            ColumnDAO.Updated(updatedColumnRec, etag)
          case DataCoordinatorClient.SchemaOutOfDateResult(realSchema) =>
            store.resolveSchemaInconsistency(datasetRecord.systemId, realSchema)
            retry()
          case DataCoordinatorClient.DuplicateValuesInColumnResult(_, _, _) =>
            ColumnDAO.DuplicateValuesInColumn(columnRecord)
          case DataCoordinatorClient.ColumnExistsAlreadyResult(_, _, _) =>
            ColumnDAO.ColumnAlreadyExists(column)
          case DataCoordinatorClient.IllegalColumnIdResult(_, _) =>
            ColumnDAO.IllegalColumnId(column)
          case DataCoordinatorClient.InvalidSystemColumnOperationResult(_, _, _) =>
            ColumnDAO.InvalidSystemColumnOperation(column)
          case DataCoordinatorClient.ColumnNotFoundResult(_, _, _) =>
            ColumnDAO.ColumnNotFound(column)
          case DataCoordinatorClient.CannotDeleteRowIdResult(_) =>
            throw new Exception("Got a 'cannot delete row id' response from a column udpate?")
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            ColumnDAO.DatasetNotFound(datasetRecord.resourceName)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            ColumnDAO.InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            ColumnDAO.InternalServerError("unknown", tag, x.toString)
        }
      }
    }

    ColumnDAO.Updated(columnRecord.copy(fieldName = spec.fieldName.getOrElse(columnRecord.fieldName)),
                      None)
  }

  def getDependencies(datasetRecord: DatasetRecord, id: ColumnId): Seq[ColumnName] =
    datasetRecord.columns.filter { col =>
      col.computationStrategy.nonEmpty &&
      col.computationStrategy.get.sourceColumns.nonEmpty &&
      col.computationStrategy.get.sourceColumns.get.exists(_.id == id)
    }.map(_.fieldName)

  def deleteColumn(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], column: ColumnName, requestId: RequestId): Result = {
    retryable(limit = 3) {
      store.lookupDataset(dataset, Some(Latest)) match {
        case Some(datasetRecord) =>
          datasetRecord.columnsByName.get(column) match {
            case Some(columnRef) =>
              val deps = getDependencies(datasetRecord, columnRef.id)

              if (deps.nonEmpty) {
                ColumnDAO.ColumnHasDependencies(columnRef.fieldName, deps)
              } else {
                val extraHeaders = SodaUtils.traceHeaders(requestId, dataset)
                dc.update(datasetRecord.handle,
                          datasetRecord.schemaHash,
                          expectedDataVersion,
                          user,
                          Iterator.single(DropColumnInstruction(columnRef.id))) {
                  case DataCoordinatorClient.NonCreateScriptResult(_, etag, copyNumber, newVersion, lastModified) =>
                    store.dropColumn(datasetRecord.systemId, columnRef.id, copyNumber, datasetRecord.primaryKey)
                    store.updateVersionInfo(datasetRecord.systemId,
                                            newVersion,
                                            lastModified,
                                            None,
                                            copyNumber,
                                            None)
                    ColumnDAO.Deleted(columnRef, etag)
                  case DataCoordinatorClient.SchemaOutOfDateResult(realSchema) =>
                    store.resolveSchemaInconsistency(datasetRecord.systemId, realSchema)
                    retry()
                  case DataCoordinatorClient.DuplicateValuesInColumnResult(_, _, _) =>
                    ColumnDAO.DuplicateValuesInColumn(columnRef)
                  case DataCoordinatorClient.ColumnExistsAlreadyResult(_, _, _) =>
                    ColumnDAO.ColumnAlreadyExists(column)
                  case DataCoordinatorClient.IllegalColumnIdResult(_, _) =>
                    ColumnDAO.IllegalColumnId(column)
                  case DataCoordinatorClient.InvalidSystemColumnOperationResult(_, _, _) =>
                    ColumnDAO.InvalidSystemColumnOperation(column)
                  case DataCoordinatorClient.ColumnNotFoundResult(_, _, _) =>
                    ColumnDAO.ColumnNotFound(column)
                  case DataCoordinatorClient.CannotDeleteRowIdResult(_) =>
                    ColumnDAO.CannotDeleteRowId(columnRef, "DELETE")
                  case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                    ColumnDAO.DatasetNotFound(dataset)
                  case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                    ColumnDAO.InternalServerError(code, tag, data)
                  case x =>
                    log.warn("case is NOT implemented %s".format(x.toString))
                    ColumnDAO.InternalServerError("unknown", tag, x.toString)
                }
              }
            case None =>
              ColumnDAO.ColumnNotFound(column)
          }
        case None =>
          ColumnDAO.DatasetNotFound(dataset)
      }
    }
  }

  def getColumn(dataset: ResourceName, column: ColumnName): Result = {
    store.lookupDataset(dataset, Some(Latest)) match {
      case Some(datasetRecord) =>
        datasetRecord.columnsByName.get(column) match {
          case Some(columnRef) =>
            ColumnDAO.Found(datasetRecord, columnRef, None)
          case None =>
            ColumnDAO.ColumnNotFound(column)
        }
      case None =>
        ColumnDAO.DatasetNotFound(dataset)
    }
  }

  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

}

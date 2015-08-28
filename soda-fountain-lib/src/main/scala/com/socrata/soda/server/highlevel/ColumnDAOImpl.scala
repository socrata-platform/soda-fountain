package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast.JValue
import com.socrata.http.server.util.RequestId.{RequestId, ReqIdHeader}
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.UnrefinedUserError
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.copy.Latest
import com.socrata.soda.server.highlevel.ColumnDAO.{NonUniqueRowId, Result}
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.wiremodels.UserProvidedColumnSpec
import com.socrata.soda.server.SodaUtils
import com.socrata.soql.environment.ColumnName
import scala.util.control.ControlThrowable

// TODO: This shouldn't be referenced here.
import com.socrata.http.server.util.Precondition
import com.socrata.soda.server.persistence.{MinimalColumnRecord, DatasetRecord, NameAndSchemaStore}

class ColumnDAOImpl(dc: DataCoordinatorClient, store: NameAndSchemaStore, columnSpecUtils: ColumnSpecUtils) extends ColumnDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnDAOImpl])

  def replaceOrCreateColumn(user: String,
                            dataset: ResourceName,
                            precondition: Precondition,
                            column: ColumnName,
                            rawSpec: UserProvidedColumnSpec,
                            requestId: RequestId): ColumnDAO.Result = {
    // TODO: This really needs to be a transaction.  It WILL FAIL if a dataset frequently read is being updated, because one of the readers will have generated dummy columns as part of inconsistency resolution
    val spec = rawSpec.copy(fieldName = rawSpec.fieldName.orElse(Some(column)))
    store.lookupDataset(dataset, Some(Latest)) match {
      case Some(datasetRecord) =>
        datasetRecord.columnsByName.get(column) match {
          case Some(columnRecord) =>
            log.warn("TODO: updating existing columns not supported yet")
            ???
          case None =>
            createColumn(user, datasetRecord, precondition, column, spec, requestId)
        }
      case None =>
        ColumnDAO.DatasetNotFound(dataset)
    }
  }

  def createColumn(user: String,
                   datasetRecord: DatasetRecord,
                   precondition: Precondition,
                   column: ColumnName,
                   userProvidedSpec: UserProvidedColumnSpec,
                   requestId: RequestId): ColumnDAO.Result = {
    columnSpecUtils.freezeForCreation(datasetRecord.columnsByName.mapValues(_.id), userProvidedSpec) match {
      case ColumnSpecUtils.Success(spec) =>
        if(spec.fieldName != column) ??? // TODO: Inconsistent url/fieldname combo
        precondition.check(None, sideEffectFree = true) match {
          case Precondition.Passed =>
            val extraHeaders = Map(ReqIdHeader -> requestId,
                                   SodaUtils.ResourceHeader -> datasetRecord.resourceName.name)
            val addColumn = AddColumnInstruction(spec.datatype, spec.fieldName.name, Some(spec.id))
            dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user,
                      Iterator.single(addColumn), extraHeaders) {
              case DataCoordinatorClient.Success(report, etag, copyNumber, newVersion, lastModified) =>
                // TODO: This next line can fail if a reader has come by and noticed the new column between the dc.update and here
                store.addColumn(datasetRecord.systemId, copyNumber, spec)
                store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                log.info("column created {} {} {}", datasetRecord.systemId.toString, copyNumber.toString, column.name)
                ColumnDAO.Created(spec.asRecord, etag)
              // TODO other cases have not been implemented
              case err@x =>
                log.warn(s"case is NOT implemented ${err.getClass.getName}")
                ???
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

  def makePK(user: String, resource: ResourceName, column: ColumnName, requestId: RequestId): Result = {
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
                val extraHeaders = Map(ReqIdHeader -> requestId,
                                       SodaUtils.ResourceHeader -> resource.name)
                dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, instructions.iterator,
                          extraHeaders) {
                  case DataCoordinatorClient.Success(_, _, copyNumber, newVersion, lastModified) =>
                    store.setPrimaryKey(datasetRecord.systemId, columnRecord.id, copyNumber)
                    store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                    ColumnDAO.Updated(columnRecord, None)
                  case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
                    store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
                    retry()
                  case DataCoordinatorClient.DuplicateValuesInColumn =>
                    ColumnDAO.NonUniqueRowId(columnRecord)
                  case UnrefinedUserError(code) =>
                    ColumnDAO.UserError(code, Map.empty)
                  // TODO other cases have not been implemented
                  case _@x =>
                    log.warn("case is NOT implemented")
                    ???
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

  def updateColumn(user: String, dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result = {
    retryable(limit = 3) {
      spec match {
        case UserProvidedColumnSpec(None, fieldName, _, _, datatype, None, _) =>
          val copyNumber = store.latestCopyNumber(dataset)
          store.lookupDataset(dataset, copyNumber) match {
            case Some(datasetRecord) =>
              datasetRecord.columnsByName.get(column) match {
                case Some(columnRef) =>
                  if (datatype.exists (_ != columnRef.typ)) {
                    // TODO: Allow some datatype conversions?
                    throw new Exception("Does not support changing datatype.")
                  } else {
                    store.updateColumnFieldName(datasetRecord.systemId, columnRef.id, spec.fieldName.get, copyNumber) match {
                      case 1 =>
                        val updatedColumnRef = columnRef.copy(fieldName = spec.fieldName.get)
                        ColumnDAO.Updated(updatedColumnRef, None)
                      case n =>
                        throw new Exception("Expect 1 from update single column, got $n")
                    }
                  }
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

  def getDependencies(datasetRecord: DatasetRecord, id: ColumnId): Seq[ColumnName] =
    datasetRecord.columns.filter { col =>
      col.computationStrategy.nonEmpty &&
      col.computationStrategy.get.sourceColumns.nonEmpty &&
      col.computationStrategy.get.sourceColumns.get.exists(_.id == id)
    }.map(_.fieldName)

  def deleteColumn(user: String, dataset: ResourceName, column: ColumnName, requestId: RequestId): Result = {
    retryable(limit = 3) {
      store.lookupDataset(dataset, Some(Latest)) match {
        case Some(datasetRecord) =>
          datasetRecord.columnsByName.get(column) match {
            case Some(columnRef) =>
              val deps = getDependencies(datasetRecord, columnRef.id)

              if (deps.nonEmpty) {
                ColumnDAO.ColumnHasDependencies(columnRef.fieldName, deps)
              } else {
                val extraHeaders = Map(ReqIdHeader -> requestId,
                                       SodaUtils.ResourceHeader -> dataset.name)
                dc.update(datasetRecord.systemId,
                          datasetRecord.schemaHash,
                          user,
                          Iterator.single(DropColumnInstruction(columnRef.id)),
                          extraHeaders) {
                  case DataCoordinatorClient.Success(_, etag, copyNumber, newVersion, lastModified) =>
                    store.dropColumn(datasetRecord.systemId, columnRef.id, copyNumber)
                    store.updateVersionInfo(datasetRecord.systemId,
                                            newVersion,
                                            lastModified,
                                            None,
                                            copyNumber,
                                            None)
                    ColumnDAO.Deleted(columnRef, etag)
                  case DataCoordinatorClient.SchemaOutOfDate(realSchema) =>
                    store.resolveSchemaInconsistency(datasetRecord.systemId, realSchema)
                    retry()
                  case DataCoordinatorClient.CannotDeleteRowId =>
                    ColumnDAO.InvalidRowIdOperation(columnRef, "DELETE")
                  // TODO other cases have not been implemented
                  case _@x =>
                    log.warn("case is NOT implemented")
                    ???
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
}

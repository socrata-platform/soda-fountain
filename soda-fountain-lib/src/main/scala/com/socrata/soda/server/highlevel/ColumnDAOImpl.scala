package com.socrata.soda.server.highlevel

import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.highlevel.ColumnDAO.Result
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.wiremodels.UserProvidedColumnSpec
import com.socrata.soql.environment.ColumnName
import scala.util.control.ControlThrowable
import com.socrata.soda.server.copy.Latest

// TODO: This shouldn't be referenced here.
import com.socrata.http.server.util.Precondition
import com.socrata.soda.server.persistence.{DatasetRecord, NameAndSchemaStore}

class ColumnDAOImpl(dc: DataCoordinatorClient, store: NameAndSchemaStore, columnSpecUtils: ColumnSpecUtils) extends ColumnDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnDAOImpl])

  def replaceOrCreateColumn(user: String, dataset: ResourceName, precondition: Precondition, column: ColumnName, rawSpec: UserProvidedColumnSpec): ColumnDAO.Result = {
    log.info("TODO: This really needs to be a transaction.  It WILL FAIL if a dataset frequently read is being updated, because one of the readers will have generated dummy columns as part of inconsistency resolution")
    val spec = rawSpec.copy(fieldName = rawSpec.fieldName.orElse(Some(column)))
    store.lookupDataset(dataset, Some(Latest)) match {
      case Some(datasetRecord) =>
        datasetRecord.columnsByName.get(column) match {
          case Some(columnRecord) =>
            log.info("TODO: updating existing columns")
            ???
          case None =>
            createColumn(user, datasetRecord, precondition, column, spec)
        }
      case None =>
        ColumnDAO.DatasetNotFound(dataset)
    }
  }

  def createColumn(user: String, datasetRecord: DatasetRecord, precondition: Precondition, column: ColumnName, userProvidedSpec: UserProvidedColumnSpec): ColumnDAO.Result = {
    columnSpecUtils.freezeForCreation(datasetRecord.columnsByName.mapValues(_.id), userProvidedSpec) match {
      case ColumnSpecUtils.Success(spec) =>
        if(spec.fieldName != column) ??? // TODO: Inconsistent url/fieldname combo
        precondition.check(None, sideEffectFree = true) match {
          case Precondition.Passed =>
            dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, Iterator.single(AddColumnInstruction(spec.datatype, spec.fieldName.name, Some(spec.id)))) {
              case DataCoordinatorClient.Success(report, etag, copyNumber, newVersion, lastModified) =>
                log.info("TODO: This next line can fail if a reader has come by and noticed the new column between the dc.update and here")
                store.addColumn(datasetRecord.systemId, copyNumber, spec)
                store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                log.info("column created {} {} {}", datasetRecord.systemId.toString, copyNumber.toString, column.name)
                ColumnDAO.Created(spec, etag)
            }
          case f: Precondition.Failure =>
            ColumnDAO.PreconditionFailed(f)
        }
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

  def makePK(user: String, resource: ResourceName, column: ColumnName): Result = {
    retryable(limit = 5) {
      store.lookupDataset(resource, Some(Latest)) match {
        case Some(datasetRecord) =>
          datasetRecord.columnsByName.get(column) match {
            case Some(columnRecord) =>
              if(datasetRecord.primaryKey == columnRecord.id) {
                ColumnDAO.Updated(columnRecord.asSpec, None)
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
                dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, instructions.iterator) {
                  case DataCoordinatorClient.Success(_, _, copyNumber, newVersion, lastModified) =>
                    store.setPrimaryKey(datasetRecord.systemId, columnRecord.id, copyNumber)
                    store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                    ColumnDAO.Updated(columnRecord.asSpec, None)
                  case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
                    store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
                    retry()
                  case DataCoordinatorClient.UpsertUserError(code, data) if code == "update.row-identifier.duplicate-values" =>
                    ColumnDAO.NonUniqueRowId(columnRecord.asSpec)
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
                        ColumnDAO.Updated(updatedColumnRef.asSpec, None)
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


  def deleteColumn(user: String, dataset: ResourceName, column: ColumnName): Result = {
    retryable(limit = 3) {
      store.lookupDataset(dataset, Some(Latest)) match {
        case Some(datasetRecord) =>
          datasetRecord.columnsByName.get(column) match {
            case Some(columnRef) =>
              dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, Iterator.single(DropColumnInstruction(columnRef.id))) {
                case DataCoordinatorClient.Success(_, etag, copyNumber, newVersion, lastModified) =>
                  store.dropColumn(datasetRecord.systemId, columnRef.id, copyNumber)
                  store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                  ColumnDAO.Deleted(columnRef.asSpec, etag)
                case DataCoordinatorClient.SchemaOutOfDate(realSchema) =>
                  store.resolveSchemaInconsistency(datasetRecord.systemId, realSchema)
                  retry()
                case DataCoordinatorClient.CannotDeleteRowId =>
                  ColumnDAO.InvalidRowIdOperation(columnRef.asSpec, "DELETE")
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
            ColumnDAO.Found(columnRef.asSpec, None)
          case None =>
            ColumnDAO.ColumnNotFound(column)
        }
      case None =>
        ColumnDAO.DatasetNotFound(dataset)
    }
  }
}

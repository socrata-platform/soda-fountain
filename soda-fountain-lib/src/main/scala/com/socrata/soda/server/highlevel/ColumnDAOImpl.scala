package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.highlevel.ColumnDAO.Result
import com.socrata.soda.server.wiremodels.UserProvidedColumnSpec // TODO: This shouldn't be referenced here.
import com.socrata.http.server.util.Precondition
import com.socrata.soda.server.persistence.{DatasetRecord, NameAndSchemaStore}
import com.socrata.soda.clients.datacoordinator.{AddColumnInstruction, DataCoordinatorClient}

class ColumnDAOImpl(dc: DataCoordinatorClient, store: NameAndSchemaStore, columnSpecUtils: ColumnSpecUtils) extends ColumnDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnDAOImpl])
  def user = {
    log.info("Actually get user info from somewhere")
    "soda-fountain"
  }

  def replaceOrCreateColumn(dataset: ResourceName, precondition: Precondition, column: ColumnName, rawSpec: UserProvidedColumnSpec): ColumnDAO.Result = {
    log.info("TODO: This really needs to be a transaction.  It WILL FAIL if a dataset frequently read is being updated, because one of the readers will have generated dummy columns as part of inconsistency resolution")
    val spec = rawSpec.copy(fieldName = rawSpec.fieldName.orElse(Some(column)))
    store.lookupDataset(dataset) match {
      case Some(datasetRecord) =>
        datasetRecord.columnsByName.get(column) match {
          case Some(columnRecord) =>
            log.info("TODO: updating existing columns")
            ???
          case None =>
            createColumn(datasetRecord, precondition, column, spec)
        }
      case None =>
        ColumnDAO.DatasetNotFound(dataset)
    }
  }

  def createColumn(datasetRecord: DatasetRecord, precondition: Precondition, column: ColumnName, userProvidedSpec: UserProvidedColumnSpec): ColumnDAO.Result = {
    columnSpecUtils.freezeForCreation(datasetRecord.columnsByName.mapValues(_.id), userProvidedSpec) match {
      case ColumnSpecUtils.Success(spec) =>
        if(spec.fieldName != column) ??? // TODO: Inconsistent url/fieldname combo
        precondition.check(None, sideEffectFree = true) match {
          case Precondition.Passed =>
            dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, Iterator.single(AddColumnInstruction(spec.datatype, spec.fieldName.name, Some(spec.id)))) {
              case DataCoordinatorClient.Success(report, etag) =>
                log.info("TODO: This next line can fail if a reader has come by and noticed the new column between the dc.update and here")
                store.addColumn(datasetRecord.systemId, spec)
                ColumnDAO.Created(spec, etag)
            }
          case f: Precondition.Failure =>
            ColumnDAO.PreconditionFailed(f)
        }
    }
  }


  def updateColumn(dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result = ???

  def deleteColumn(dataset: ResourceName, column: ColumnName): Result = ???

  def getColumn(dataset: ResourceName, column: ColumnName): Result = ???
}

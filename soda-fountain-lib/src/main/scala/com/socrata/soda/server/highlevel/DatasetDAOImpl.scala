package com.socrata.soda.server.highlevel

import com.socrata.soda.clients.datacoordinator.{SetRowIdColumnInstruction, AddColumnInstruction, DataCoordinatorClient}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.DatasetDAO.Result
import com.socrata.soda.server.wiremodels.{ColumnSpec, DatasetSpec, UserProvidedDatasetSpec}
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soql.brita.IdentifierFilter
import DatasetDAO._
import scala.util.{Success, Failure}
import com.socrata.soql.environment.ColumnName

class DatasetDAOImpl(dc: DataCoordinatorClient, store: NameAndSchemaStore, columnSpecUtils: ColumnSpecUtils) extends DatasetDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetDAOImpl])
  val defaultDescription = ""
  val defaultLocale = "en_US"
  def user = {
    log.info("Actually get user info from somewhere")
    "soda-fountain"
  }

  def validResourceName(rn: ResourceName) = IdentifierFilter(rn.name) == rn.name

  def createDataset(spec: UserProvidedDatasetSpec): Result = spec match {
    case UserProvidedDatasetSpec(Some(resourceName), Some(name), description, rowIdentifier, locale, columns) =>
      val trueDesc = description.getOrElse(defaultDescription)
      val trueRID = rowIdentifier.flatten
      val trueLocale = locale.flatten.getOrElse(defaultLocale)
      val trueColumns = columns.getOrElse(Seq.empty).foldLeft(Map.empty[ColumnName, ColumnSpec]) { (acc, userColumnSpec) =>
        columnSpecUtils.freezeForCreation(acc.mapValues(_.id), userColumnSpec) match {
          case ColumnSpecUtils.Success(cSpec) => acc + (cSpec.fieldName -> cSpec)
        }
      }

      createDataset(DatasetSpec(resourceName, name, trueDesc, trueRID, trueLocale, trueColumns))
  }

  def createDataset(spec: DatasetSpec): Result = {
    if(!validResourceName(spec.resourceName)) return InvalidDatasetName(spec.resourceName)
    store.translateResourceName(spec.resourceName) match {
      case None =>
        val addRidInstruction = spec.rowIdentifier.map { ridFieldName =>
          if(!spec.columns.contains(ridFieldName)) return NonexistantColumn(ridFieldName)
          new SetRowIdColumnInstruction(spec.columns(ridFieldName).id)
        }
        // ok cool.  First send it upstream, then if that works stick it in the store.
        val columnInstructions = spec.columns.values.map { c => new AddColumnInstruction(c.dataType, c.fieldName.name, Some(c.id)) }

        val instructions = columnInstructions ++ addRidInstruction

        dc.create(user, Some(instructions.iterator), spec.locale) match {
          case Success((datasetId, _)) =>
            log.info("TODO: Need to store the name and description somewhere too!")
            log.info("TODO: Also need to store column names and descriptions somewhere!")
            store.addResource(spec.resourceName, datasetId, spec.columns.mapValues(_.id))
            Created(spec)
          case Failure(t) =>
            throw t
        }
      case Some(_) =>
        DatasetAlreadyExists(spec.resourceName)
    }
  }

  def replaceOrCreateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result = ???

  def updateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result = ???

  def deleteDataset(dataset: ResourceName): Result = ???

  def getDataset(dataset: ResourceName): Result = ???
}

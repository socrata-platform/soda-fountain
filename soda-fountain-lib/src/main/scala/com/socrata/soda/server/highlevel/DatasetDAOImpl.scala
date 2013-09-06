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

  def freezeForCreation(spec: UserProvidedDatasetSpec): Either[Result, DatasetSpec] = spec match {
    case UserProvidedDatasetSpec(Some(resourceName), Some(name), description, rowIdentifier, locale, columns) =>
      val trueDesc = description.getOrElse(defaultDescription)
      val trueRID = rowIdentifier.flatten
      val trueLocale = locale.flatten.getOrElse(defaultLocale)
      val trueColumns = columns.getOrElse(Seq.empty).foldLeft(Map.empty[ColumnName, ColumnSpec]) { (acc, userColumnSpec) =>
        columnSpecUtils.freezeForCreation(acc.mapValues(_.id), userColumnSpec) match {
          case ColumnSpecUtils.Success(cSpec) => acc + (cSpec.fieldName -> cSpec)
          // TODO: not-success case
        }
      }
      Right(DatasetSpec(resourceName, name, trueDesc, trueRID, trueLocale, trueColumns))
    // TODO: Not-success case
  }

  def createDataset(spec: UserProvidedDatasetSpec): Result =
    freezeForCreation(spec) match {
      case Right(frozenSpec) => createDataset(frozenSpec)
      case Left(result) => result
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
        val columnInstructions = spec.columns.values.map { c => new AddColumnInstruction(c.datatype, c.fieldName.name, Some(c.id)) }

        val instructions = columnInstructions ++ addRidInstruction

        val (datasetId, _) = dc.create(user, Some(instructions.iterator), spec.locale)
        // TODO: we should store system column info too
        store.addResource(datasetId, spec)
        Created(spec)
      case Some(_) =>
        DatasetAlreadyExists(spec.resourceName)
    }
  }

  def replaceOrCreateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result =
    store.translateResourceName(dataset) match {
      case None =>
        NotFound(dataset)
      case Some((datasetId, schemaIsh)) =>
        val oldLocale = log.info("TODO: Need to get the locale from somewhere")
        freezeForCreation(spec) match {
          case Right(frozenSpec) =>
            if(oldLocale != frozenSpec.locale) return LocaleChanged
            // ok.  What we need to do now is turn this into a list of operations to hand
            // off to the data coordinator and/or our database.  This will include:
            //  1. getting the current schema from the D.C. or cache
            //  2. Figuring out what columns need to be renamed (note: this may include cycles!) and/or added or deleted.
            //  3. Issuing column updates (add/delete) to the D.C.  This may fail with a "schema out of date"
            //     error; if so go back to step 1.
            //  4. update our representation of the schema (i.e., rename columns) and save name, description changes.
            //
            // TODO: Figure out what happens in the event of inconsistency!!!
            ???
          case Left(result) =>
            result
        }
    }

  def updateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result = ???

  def deleteDataset(dataset: ResourceName): Result = ???

  def getDataset(dataset: ResourceName): Result =
    store.lookupDataset(dataset) match {
      case Some(datasetRecord) =>
        // TODO: Figure out what happens in the event of inconsistency!!!
        dc.getSchema(datasetRecord.systemId) match {
          case Some(schemaSpec) =>
            val spec = DatasetSpec(
              datasetRecord.resourceName,
              datasetRecord.name,
              datasetRecord.description,
              datasetRecord.columnsById.get(schemaSpec.pk).map(_.fieldName), // TODO this is one of the potential inconsistencies
              schemaSpec.locale,
              datasetRecord.columnsByName.mapValues { cr =>
                ColumnSpec(cr.id, cr.fieldName, cr.name, cr.description, schemaSpec.schema(cr.id)) // TODO this is another
              })
            Found(spec)
          case None =>
            // this is one of those inconsistencies.
            // we should probably delete the dataset.
            // But for now just scream and die
            throw new Exception("Cannot find " + datasetRecord.systemId + " (" + dataset + ") in data-coordinator?!")
        }
      case None =>
        NotFound(dataset)
    }
}

package com.socrata.soda.server.highlevel

import com.socrata.soda.clients.datacoordinator.{SetRowIdColumnInstruction, AddColumnInstruction, DataCoordinatorClient}
import com.socrata.soda.server.id.{SecondaryId, ColumnId, ResourceName}
import com.socrata.soda.server.highlevel.DatasetDAO.Result
import com.socrata.soda.server.wiremodels.{ColumnSpec, DatasetSpec, UserProvidedDatasetSpec}
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soql.brita.IdentifierFilter
import DatasetDAO._
import scala.util.{Success, Failure}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLVersion, SoQLFixedTimestamp, SoQLID, SoQLType}
import scala.util.control.ControlThrowable

class DatasetDAOImpl(dc: DataCoordinatorClient, store: NameAndSchemaStore, columnSpecUtils: ColumnSpecUtils, instanceForCreate: () => String) extends DatasetDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetDAOImpl])
  val defaultDescription = ""
  val defaultPrimaryKey = ColumnName(":id")
  val defaultLocale = "en_US"
  def user = {
    log.info("Actually get user info from somewhere")
    "soda-fountain"
  }

  def validResourceName(rn: ResourceName) = IdentifierFilter(rn.name) == rn.name

  def freezeForCreation(spec: UserProvidedDatasetSpec): Either[Result, DatasetSpec] = spec match {
    case UserProvidedDatasetSpec(Some(resourceName), Some(name), description, rowIdentifier, locale, columns) =>
      val trueDesc = description.getOrElse(defaultDescription)
      val trueRID = rowIdentifier.getOrElse(defaultPrimaryKey)
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
        // sid col is a little painful; if it's ":id" we want to do NOTHING.
        // If it's a system column OTHER THAN :id we want to add the set-row-id instruction but NOT look it up in the spec
        // If it's anythign else we want to ensure it's in the spec AND issue the set-row-id-instruction
        val ridFieldName = spec.rowIdentifier
        val addRidInstruction =
          if(ridFieldName == defaultPrimaryKey) {
            Nil
          } else if(systemColumns.contains(ridFieldName)) {
            List(new SetRowIdColumnInstruction(systemColumns(ridFieldName).id))
          } else {
            if(!spec.columns.contains(ridFieldName)) return NonexistantColumn(ridFieldName)
            List(new SetRowIdColumnInstruction(spec.columns(ridFieldName).id))
          }
        // ok cool.  First send it upstream, then if that works stick it in the store.
        val columnInstructions = spec.columns.values.map { c => new AddColumnInstruction(c.datatype, c.fieldName.name, Some(c.id)) }

        val instructions = columnInstructions ++ addRidInstruction

        val (datasetId, _) = dc.create(instanceForCreate(), user, Some(instructions.iterator), spec.locale)

        val trueSpec = spec.copy(columns = spec.columns ++ systemColumns)
        val record = trueSpec.asRecord(datasetId)

        // sanity-check
        locally {
          val dcSchema = dc.getSchema(datasetId)
          val mySchema = record.schemaSpec
          assert(dcSchema == Some(mySchema), "Schema spec differs between DC and me!:\n" + dcSchema + "\n" + mySchema)
        }

        store.addResource(record)
        Created(trueSpec)
      case Some(_) =>
        DatasetAlreadyExists(spec.resourceName)
    }
  }

  def replaceOrCreateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        freezeForCreation(spec) match {
          case Right(frozenSpec) =>
            if(datasetRecord.locale != frozenSpec.locale) return LocaleChanged
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
      case None =>
        NotFound(dataset)
    }

  def updateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result = ???

  def deleteDataset(dataset: ResourceName): Result = ???

  def getVersion(dataset: ResourceName, secondary: SecondaryId): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          val vr = dc.checkVersionInSecondary(datasetRecord.systemId, secondary)
          DatasetVersion(vr)
        case None =>
          NotFound(dataset)
      }
    }

  def getDataset(dataset: ResourceName): Result =
    store.lookupDataset(dataset) match {
      case Some(datasetRecord) =>
        val spec = DatasetSpec(
          datasetRecord.resourceName,
          datasetRecord.name,
          datasetRecord.description,
          datasetRecord.columnsById(datasetRecord.primaryKey).fieldName,
          datasetRecord.locale,
          datasetRecord.columnsByName.mapValues { cr =>
            ColumnSpec(cr.id, cr.fieldName, cr.name, cr.description, cr.typ)
          })
        Found(spec)
      case None =>
        NotFound(dataset)
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

  def makeCopy(dataset: ResourceName, copyData: Boolean): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.copy(datasetRecord.systemId, datasetRecord.schemaHash, copyData, user) {
            case DataCoordinatorClient.Success(_) =>
              WorkingCopyCreated
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
          }
        case None =>
          NotFound(dataset)
      }
    }

  def dropCurrentWorkingCopy(dataset: ResourceName): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.dropCopy(datasetRecord.systemId, datasetRecord.schemaHash, user) {
            case DataCoordinatorClient.Success(_) =>
              WorkingCopyDropped
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
          }
        case None =>
          NotFound(dataset)
      }
    }

  def publish(dataset: ResourceName, snapshotLimit: Option[Int]): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.publish(datasetRecord.systemId, datasetRecord.schemaHash, snapshotLimit, user) {
            case DataCoordinatorClient.Success(_) =>
              WorkingCopyPublished
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
          }
        case None =>
          NotFound(dataset)
      }
    }

  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.propagateToSecondary(datasetRecord.systemId, secondary)
          PropagatedToSecondary
        case None =>
          NotFound(dataset)
      }
    }

  private[this] val _systemColumns = Map(
    ColumnName(":id") -> ColumnSpec(ColumnId(":id"), ColumnName(":id"), ":id", "", SoQLID),
    ColumnName(":version") -> ColumnSpec(ColumnId(":version"), ColumnName(":version"), ":version", "", SoQLVersion),
    ColumnName(":created_at") -> ColumnSpec(ColumnId(":created_at"), ColumnName(":created_at"), ":created_at", "", SoQLFixedTimestamp),
    ColumnName(":updated_at") -> ColumnSpec(ColumnId(":updated_at"), ColumnName(":updated_at"), ":updated_at", "", SoQLFixedTimestamp)
  )

  private[this] def systemColumns = {
    log.info("TODO: Grovel system columns out of the data coordinator instead of hardcoding")
    _systemColumns
  }
}

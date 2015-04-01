package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator.{DropRollupInstruction, CreateOrUpdateRollupInstruction, SetRowIdColumnInstruction, AddColumnInstruction, DataCoordinatorClient}
import com.socrata.soda.server.id.{ColumnId, SecondaryId, ResourceName, RollupName}
import com.socrata.soda.server.persistence.{MinimalDatasetRecord, NameAndSchemaStore}
import com.socrata.soda.server.wiremodels.{UserProvidedRollupSpec, ColumnSpec, DatasetSpec, UserProvidedDatasetSpec}
import com.socrata.soda.server.SodaUtils.traceHeaders
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.exceptions.{NoSuchColumn, SoQLException}
import com.socrata.soql.mapping.ColumnNameMapper
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.types.{SoQLType, SoQLAnalysisType}
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.functions.SoQLFunctionInfo
import com.socrata.soql.functions.SoQLTypeInfo
import DatasetDAO._
import scala.util.control.ControlThrowable
import com.socrata.soda.server.copy.{Discarded, Published, Unpublished, Stage}

class DatasetDAOImpl(dc: DataCoordinatorClient, store: NameAndSchemaStore, columnSpecUtils: ColumnSpecUtils, instanceForCreate: () => String) extends DatasetDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetDAOImpl])
  val defaultDescription = ""
  val defaultPrimaryKey = ColumnName(":id")
  val defaultLocale = "en_US"

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
          // TODO other cases have not been implemented
          case _@x =>
            log.warn("case is NOT implemented")
            ???
        }
      }
      Right(DatasetSpec(resourceName, name, trueDesc, trueRID, trueLocale, None, trueColumns))
    // TODO: Not-success case
  }

  def createDataset(user: String, spec: UserProvidedDatasetSpec, requestId: RequestId): Result =
    freezeForCreation(spec) match {
      case Right(frozenSpec) => createDataset(user, frozenSpec, requestId)
      case Left(result) => result
    }

  def createDataset(user: String, spec: DatasetSpec, requestId: RequestId): Result = {
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
          } else if(columnSpecUtils.systemColumns.contains(ridFieldName)) {
            List(new SetRowIdColumnInstruction(columnSpecUtils.systemColumns(ridFieldName).id))
          } else {
            if(!spec.columns.contains(ridFieldName)) return NonexistantColumn(ridFieldName)
            List(new SetRowIdColumnInstruction(spec.columns(ridFieldName).id))
          }
        // ok cool.  First send it upstream, then if that works stick it in the store.
        val columnInstructions = spec.columns.values.map { c => new AddColumnInstruction(c.datatype, c.fieldName.name, Some(c.id)) }

        val instructions = columnInstructions ++ addRidInstruction

        val (reportMetaData, _) = dc.create(instanceForCreate(),
                                            user,
                                            Some(instructions.iterator),
                                            spec.locale,
                                            traceHeaders(requestId, spec.resourceName))

        val trueSpec = spec.copy(columns = spec.columns ++ columnSpecUtils.systemColumns)
        val record = trueSpec.asRecord(reportMetaData)

        // sanity-check
        locally {
          val dcSchema = dc.getSchema(reportMetaData.datasetId)
          val mySchema = record.schemaSpec
          assert(dcSchema == Some(mySchema), "Schema spec differs between DC and me!:\n" + dcSchema + "\n" + mySchema)
        }

        store.addResource(record)
        store.updateVersionInfo(reportMetaData.datasetId, reportMetaData.version, reportMetaData.lastModified, None, Stage.InitialCopyNumber, None)
        Created(trueSpec.asRecord(reportMetaData))
      case Some(_) =>
        DatasetAlreadyExists(spec.resourceName)
    }
  }

  def replaceOrCreateDataset(user: String,
                             dataset: ResourceName,
                             spec: UserProvidedDatasetSpec,
                             requestId: RequestId): Result =
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

  def updateDataset(user: String,
                    dataset: ResourceName,
                    spec: UserProvidedDatasetSpec,
                    requestId: RequestId): Result = ???

  def deleteDataset(user: String, dataset: ResourceName, requestId: RequestId): Result = {
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.deleteAllCopies(datasetRecord.systemId, datasetRecord.schemaHash, user,
                             traceHeaders(requestId, dataset)) {
            case DataCoordinatorClient.Success(_, _, _, _, _) =>
              store.removeResource(dataset)
              Deleted
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            // TODO other cases have not been implemented
            case _@x =>
              log.warn("case is NOT implemented")
              ???
          }
        case None =>
          NotFound(dataset)
      }
    }
  }

  def getVersion(dataset: ResourceName, secondary: SecondaryId, requestId: RequestId): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        val vr = dc.checkVersionInSecondary(datasetRecord.systemId,
                                            secondary,
                                            traceHeaders(requestId, dataset))
        DatasetVersion(vr)
      case None =>
        NotFound(dataset)
    }

  def getCurrentCopyNum(dataset: ResourceName): Option[Long] =
    store.lookupCopyNumber(dataset, Some(Published)) match {
      case None => store.lookupCopyNumber(dataset, Some(Unpublished))
      case o: Some[Long] => o
    }

  def getDataset(dataset: ResourceName, stage: Option[Stage]): Result =
    store.lookupDataset(dataset, stage) match {
      case Some(datasetRecord) => Found(datasetRecord)
      case None                => NotFound(dataset)
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

  def makeCopy(user: String, dataset: ResourceName, copyData: Boolean, requestId: RequestId): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.copy(datasetRecord.systemId, datasetRecord.schemaHash, copyData, user,
                  extraHeaders = traceHeaders(requestId, dataset)) {
            case DataCoordinatorClient.Success(_, _, newCopyNumber, newVersion, lastModified) =>
              store.makeCopy(datasetRecord.systemId, newCopyNumber, newVersion)
              WorkingCopyCreated
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            // TODO other cases have not been implemented
            case _@x =>
              log.warn("case is NOT implemented")
              ???
          }
        case None =>
          NotFound(dataset)
      }
    }

  def dropCurrentWorkingCopy(user: String, dataset: ResourceName, requestId: RequestId): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          // Cannot use copy number from DC because it indicates the latest surviving copy.
          store.lookupCopyNumber(dataset, Some(Unpublished)) match {
            case Some(unpublishCopyNumber) =>
              dc.dropCopy(datasetRecord.systemId, datasetRecord.schemaHash, user,
                          extraHeaders = traceHeaders(requestId, dataset)) {
                case DataCoordinatorClient.Success(_, _, _, newVersion, lastModified) =>
                  store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, Some(Discarded), unpublishCopyNumber, None)
                  WorkingCopyDropped
                case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
                  store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
                  retry()
                // TODO other cases have not been implemented
                case _@x =>
                  log.warn("case is NOT implemented")
                  ???
              }
            case None =>
              NotFound(dataset)
          }
        case None =>
          NotFound(dataset)
      }
    }

  def publish(user: String, dataset: ResourceName, snapshotLimit: Option[Int], requestId: RequestId): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.publish(datasetRecord.systemId, datasetRecord.schemaHash, snapshotLimit, user,
                     extraHeaders = traceHeaders(requestId, dataset)) {
            case DataCoordinatorClient.Success(_, _, copyNumber, newVersion, lastModified) =>
              store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, Some(Published), copyNumber, snapshotLimit)
              WorkingCopyPublished
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            // TODO other cases have not been implemented
            case _@x =>
              log.warn("case is NOT implemented")
              ???
          }
        case None =>
          NotFound(dataset)
      }
    }

  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId, requestId: RequestId): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.propagateToSecondary(datasetRecord.systemId, secondary,
                                traceHeaders(requestId, dataset))
        PropagatedToSecondary
      case None =>
        NotFound(dataset)
    }


  /**
   * Maps a rollup definition column name to column id that can be used by lower layers
   * that don't know about column mappings.  We need to ensure the names are valid soql,
   * so if it isn't a system column we prefix a "_".  The specific case we are worried
   * about is when it is a 4x4 that starts with a number.
   */
  private def rollupColumnNameToIdMapping(cid: ColumnId): ColumnName = {
    val name = cid.underlying
    name(0) match {
      case ':' => new ColumnName(name)
      case _ => new ColumnName("_" + name)
    }
  }

  def replaceOrCreateRollup(user: String,
                            dataset: ResourceName,
                            rollup: RollupName,
                            spec: UserProvidedRollupSpec,
                            requestId: RequestId): Result =
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          spec.soql match {
            case Some(soql) =>
              // We don't actually need the analysis, we are just running it here so we
              // can give feedback to the API caller if something is wrong with the query.
              analyzeQuery(datasetRecord, spec.soql.get) match {
                case Left(result) => result
                case Right(_) =>
                  val columnNameMap = datasetRecord.columnsByName.mapValues(col => rollupColumnNameToIdMapping(col.id))

                  val parsedQuery = new StandaloneParser().selectStatement(soql)
                  val mappedQuery = new ColumnNameMapper(columnNameMap).mapSelect(parsedQuery)
                  log.debug(s"soql for rollup ${rollup} is: ${parsedQuery}")
                  log.debug(s"Mapped soql for rollup ${rollup} is: ${mappedQuery}")

                  val instruction = CreateOrUpdateRollupInstruction(rollup, mappedQuery.toString())
                  dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user,
                            Iterator.single(instruction), traceHeaders(requestId, dataset)) {
                    // TODO better support for error handling in various failure cases
                    case DataCoordinatorClient.Success(report, etag, copyNumber, newVersion, lastModified) =>
                      store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                      RollupCreatedOrUpdated
                    // TODO other cases have not been implemented
                    case _@x =>
                      log.warn("case is NOT implemented")
                      ???
                  }
              }
            case None =>
              RollupError("soql field missing")
          }
        case None =>
          NotFound(dataset)
      }

  private def analyzeQuery(ds: MinimalDatasetRecord, query: String): Either[Result, SoQLAnalysis[ColumnName, SoQLAnalysisType]] = {
    val columnIdMap: Map[ColumnName, String] = ds.columnsByName.mapValues(_.id.underlying)
    val rawSchema: Map[String, SoQLType] = ds.schemaSpec.schema.map { case (k, v) => (k.underlying, v) }

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

    val dsCtx = new DatasetContext[SoQLAnalysisType] {
      val schema = OrderedMap(columnIdMap.mapValues(rawSchema).toSeq.sortBy(_._1) : _*)
    }
    try {
      val analysis = analyzer.analyzeFullQuery(query)(dsCtx)
      log.debug(s"Rollup analysis successful: ${analysis}")
      Right(analysis)
    } catch {
      case NoSuchColumn(name, _) => Left(RollupColumnNotFound(name))
      case e: SoQLException => Left(RollupError(e.getMessage))
    }
  }

  // TODO implement
  def getRollup(user: String, dataset: ResourceName, rollup: RollupName, requestId: RequestId): Result = ???

  def deleteRollup(user: String, dataset: ResourceName, rollup: RollupName, requestId: RequestId): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        val instruction = DropRollupInstruction(rollup)

        dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user,
                  Iterator.single(instruction), traceHeaders(requestId, dataset)) {
          // TODO better support for error handling in various failure cases
          case DataCoordinatorClient.Success(report, etag, copyNumber, newVersion, lastModified) =>
            store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
            RollupDropped
          case DataCoordinatorClient.UpsertUserError("delete.rollup.does-not-exist", _) =>
            RollupNotFound(rollup)
          // TODO other cases have not been implemented
          case _@x =>
            log.warn("case is NOT implemented")
            ???
        }
      case None =>
        NotFound(dataset)
    }
  }
}

package com.socrata.soda.server.highlevel

import com.socrata.http.server.util.RequestId
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.highlevel.DatasetDAO.CannotAcquireDatasetWriteLock
import com.socrata.soda.server.highlevel.DatasetDAO.FeedbackInProgress
import com.socrata.soda.server.id._
import com.socrata.soda.server.persistence.{MinimalDatasetRecord, NameAndSchemaStore}
import com.socrata.soda.server.wiremodels._
import com.socrata.soda.server.SodaUtils.traceHeaders
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.exceptions.{NoSuchColumn, SoQLException}
import com.socrata.soql.mapping.ColumnNameMapper
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.functions.SoQLFunctionInfo
import com.socrata.soql.functions.SoQLTypeInfo
import DatasetDAO._

import scala.util.control.ControlThrowable
import com.socrata.soda.server.copy.{Discarded, Published, Stage, Unpublished}
import com.socrata.soda.server.resources.{DCCollocateOperation, SFCollocateOperation}

class DatasetDAOImpl(dc: DataCoordinatorClient,
                     fbm: FeedbackSecondaryManifestClient,
                     store: NameAndSchemaStore,
                     columnSpecUtils: ColumnSpecUtils,
                     instanceForCreate: () => String) extends DatasetDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetDAOImpl])
  val defaultDescription = ""
  val defaultPrimaryKey = ColumnName(":id")
  val defaultLocale = "en_US"

  def validResourceName(rn: ResourceName) = IdentifierFilter(rn.name) == rn.name

  // We only support updating the resource name right now. The rest is left as a TODO.
  def freezeForUpdate(spec: UserProvidedDatasetSpec): Either[Result, ResourceName] = spec match {
    case UserProvidedDatasetSpec(Some(resourceName), None, None, None, None, None) =>
      Right(resourceName)
    case other =>
      log.warn("Soda Fountain does not yet support the following patch request: " + spec)
      Left(UnsupportedUpdateOperation("Soda Fountain does not yet support the following patch request: " + spec))
  }

  def freezeForCreation(spec: UserProvidedDatasetSpec): Either[Result, DatasetSpec] = {
    spec match {
      case UserProvidedDatasetSpec(Some(resourceName), Some(name), description, rowIdentifier, locale, columns) =>
        val trueDesc = description.getOrElse(defaultDescription)
        val trueRID = rowIdentifier.getOrElse(defaultPrimaryKey)
        val trueLocale = locale.flatten.getOrElse(defaultLocale)
        val trueColumns = columns.getOrElse(Seq.empty).foldLeft(Map.empty[ColumnName, ColumnSpec]) { (acc, userColumnSpec) =>
          columnSpecUtils.freezeForCreation(acc.mapValues { col => (col.id, col.datatype) }, userColumnSpec) match {
            case ColumnSpecUtils.Success(cSpec) => acc + (cSpec.fieldName -> cSpec)
            // TODO: not-success case
            // TODO other cases have not been implemented
            case x =>
              log.warn("case is NOT implemented: %s".format(x.toString))
              ???
          }
        }
        Right(DatasetSpec(resourceName, name, trueDesc, trueRID, trueLocale, None, trueColumns))
      // TODO: Not-success case
    }
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
        // If it's anything else we want to ensure it's in the spec AND issue the set-row-id-instruction
        val ridFieldName = spec.rowIdentifier
        val addRidInstruction =
          if(ridFieldName == defaultPrimaryKey) {
            Nil
          } else if(columnSpecUtils.systemColumns.contains(ridFieldName)) {
            List(new SetRowIdColumnInstruction(columnSpecUtils.systemColumns(ridFieldName).id))
          } else {
            if(!spec.columns.contains(ridFieldName)) return NonExistentColumn(spec.resourceName, ridFieldName)
            List(new SetRowIdColumnInstruction(spec.columns(ridFieldName).id))
          }
        // ok cool.  First send it upstream, then if that works stick it in the store.
        val columnInstructions = spec.columns.values.map { c => new AddColumnInstruction(c.datatype, c.fieldName, Some(c.id), c.computationStrategy) }

        val instructions = columnInstructions ++ addRidInstruction

        val (reportMetaData, _) = dc.create(spec.resourceName,
                                            instanceForCreate(),
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

        val strategyTypes = spec.columns.flatMap { case (_, colSpec) => colSpec.computationStrategy }.map { _.strategyType }.toSet
        fbm.maybeReplicate(record.systemId, strategyTypes, traceHeaders(requestId, record.resourceName))

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
            if(datasetRecord.locale != frozenSpec.locale) return LocaleChanged(frozenSpec.locale)
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
        DatasetNotFound(dataset)
    }

  def updateDataset(user: String,
                    dataset: ResourceName,
                    spec: UserProvidedDatasetSpec,
                    requestId: RequestId): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        freezeForUpdate(spec) match {
          case Right(resourceName) =>
            // Ensure the new resource name is valid
            if(!validResourceName(resourceName))
              return InvalidDatasetName(resourceName)

            // Ensure the new resource name doesn't collide with another dataset
            store.translateResourceName(resourceName) match {
              case Some(existing) =>
                DatasetAlreadyExists(resourceName)
              case None =>
                // Make the changes
                store.patchResource(datasetRecord.resourceName, resourceName)

                // Retrieve the updated record to send back in the response
                store.lookupDataset(resourceName).headOption match {
                  case Some(updated) =>
                    Updated(updated)
                  // The below should never happen... #lastwords
                  case None =>
                    throw new Exception("Could not find renamed dataset in metasoda")
                }
            }
          case Left(result) => result
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def removeDataset (user: String, dataset: ResourceName, requestId: RequestId): Result = {
    retryable(limit = 5) {
      store.translateResourceName(dataset, deleted = true) match {
        case Some(datasetRecord) =>
          dc.deleteAllCopies(datasetRecord.systemId, datasetRecord.schemaHash, "",
            traceHeaders(RequestId.generate(), dataset))
          {
            case DataCoordinatorClient.NonCreateScriptResult(_, _, _, _, _) =>
              store.removeResource(dataset)
              Deleted
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
              // should only have two error case for this path.
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              DatasetNotFound(dataset)
            case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
              CannotAcquireDatasetWriteLock(dataset)
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
              InternalServerError(code, tag, data)
            case x =>
              log.warn("case is NOT implemented %s".format(x.toString))
              InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }
  }

  def markDatasetForDeletion(user: String, dataset: ResourceName): Result = {
    //TODO: Ask about retryable and give a return value
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          store.markResourceForDeletion(dataset)
          Deleted
        case None =>
          DatasetNotFound(dataset)
      }
  }

  def unmarkDatasetForDeletion(user: String, dataset: ResourceName) : Result = {
    store.translateResourceName(dataset, None, true) match {
      case Some(datasetRecord) =>
        store.unmarkResourceForDeletion(dataset)
        Undeleted
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def getSecondaryVersions(dataset: ResourceName, requestId: RequestId): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.checkVersionInSecondaries(datasetRecord.systemId, traceHeaders(requestId, dataset)) match {
          case Right(vrs) => vrs.map(DatasetSecondaryVersions).getOrElse(DatasetNotFound(dataset))
          case Left(fail) => UnexpectedInternalServerResponse(fail.reason, fail.tag)
        }
      case None =>
        DatasetNotFound(dataset)
    }

  def getVersion(dataset: ResourceName, secondary: SecondaryId, requestId: RequestId): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.checkVersionInSecondary(datasetRecord.systemId, secondary, traceHeaders(requestId, dataset)) match {
          case Right(vr) => vr.map(DatasetVersion).getOrElse(DatasetNotFound(dataset))
          case Left(fail) => UnexpectedInternalServerResponse(fail.reason, fail.tag)
        }
      case None =>
        DatasetNotFound(dataset)
    }

  def getCurrentCopyNum(dataset: ResourceName): Option[Long] =
    store.lookupCopyNumber(dataset, Some(Published)) match {
      case None => store.lookupCopyNumber(dataset, Some(Unpublished))
      case o: Some[Long] => o
    }

  def getDataset(dataset: ResourceName, stage: Option[Stage]): Result =
    store.lookupDataset(dataset, stage) match {
      case Some(datasetRecord) =>
        Found(datasetRecord)
      case None =>
        DatasetNotFound(dataset)
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
            case DataCoordinatorClient.NonCreateScriptResult(_, _, newCopyNumber, newVersion, lastModified) =>
              store.makeCopy(datasetRecord.systemId, newCopyNumber, newVersion)
              WorkingCopyCreated
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              DatasetNotFound(dataset)
            case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
              CannotAcquireDatasetWriteLock(dataset)
            case DataCoordinatorClient.FeedbackInProgressResult(_, _, stores) =>
              FeedbackInProgress(dataset, stores)
            case DataCoordinatorClient.IncorrectLifecycleStageResult(actualStage: String, expectedStage: Set[String]) =>
              IncorrectLifecycleStageResult(actualStage, expectedStage)
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
              InternalServerError(code, tag, data)
            case x =>
              log.warn("case is NOT implemented %s".format(x.toString))
              InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          DatasetNotFound(dataset)
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
                case DataCoordinatorClient.NonCreateScriptResult(_, _, _, newVersion, lastModified) =>
                  store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, Some(Discarded), unpublishCopyNumber, None)
                  WorkingCopyDropped
                case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
                  store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
                  retry()
                case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                  DatasetNotFound(dataset)
                case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
                  CannotAcquireDatasetWriteLock(dataset)
                case DataCoordinatorClient.FeedbackInProgressResult(_, _, stores) =>
                  FeedbackInProgress(dataset, stores)
                case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                  InternalServerError(code, tag, data)
                case x =>
                  log.warn("case is NOT implemented %s".format(x.toString))
                  InternalServerError("unknown", tag, x.toString)
              }
            case None =>
              DatasetNotFound(dataset)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }

  def publish(user: String, dataset: ResourceName, keepSnapshot: Option[Boolean], requestId: RequestId): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.publish(datasetRecord.systemId, datasetRecord.schemaHash, keepSnapshot, user,
                     extraHeaders = traceHeaders(requestId, dataset)) {
            case DataCoordinatorClient.NonCreateScriptResult(_, _, copyNumber, newVersion, lastModified) =>
              store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, Some(Published), copyNumber, Some(0))
              WorkingCopyPublished
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              DatasetNotFound(dataset)
            case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
              CannotAcquireDatasetWriteLock(dataset)
            case DataCoordinatorClient.FeedbackInProgressResult(_, _, stores) =>
              FeedbackInProgress(dataset, stores)
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
              InternalServerError(code, tag, data)
            case x =>
              log.warn("case is NOT implemented %s".format(x.toString))
              InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }

  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId, requestId: RequestId): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.propagateToSecondary(datasetRecord.systemId, secondary,
                                traceHeaders(requestId, dataset))
        PropagatedToSecondary
      case None =>
        DatasetNotFound(dataset)
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
                  val mappedQueries = new ColumnNameMapper(columnNameMap).mapSelect(parsedQuery)
                  mappedQueries.size match {
                    case 1 =>
                      val mappedQuery = mappedQueries.head
                      log.debug(s"soql for rollup ${rollup} is: ${parsedQuery}")
                      log.debug(s"Mapped soql for rollup ${rollup} is: ${mappedQuery}")

                      val instruction = CreateOrUpdateRollupInstruction(rollup, mappedQuery.toString())
                      dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user,
                        Iterator.single(instruction), traceHeaders(requestId, dataset)) {
                        case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, lastModified) =>
                          store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
                          RollupCreatedOrUpdated
                        case DataCoordinatorClient.NoSuchRollupResult(_, _) =>
                          RollupNotFound(rollup)
                        case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                          DatasetNotFound(dataset)
                        case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
                          CannotAcquireDatasetWriteLock(dataset)
                        case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                          InternalServerError(code, tag, data)
                        case x =>
                          log.warn("case is NOT implemented %s".format(x.toString))
                          InternalServerError("unknown", tag, x.toString)
                      }
                    case _ =>
                      // It cannot get here because it is prevented by analyzeQuery which does not take chained soql.
                      RollupError("rollup soql cannot be chained")
                  }
              }
            case None =>
              RollupError("soql field missing")
          }
        case None =>
          DatasetNotFound(dataset)
      }

  private def analyzeQuery(ds: MinimalDatasetRecord, query: String): Either[Result, SoQLAnalysis[ColumnName, SoQLType]] = {
    val columnIdMap: Map[ColumnName, String] = ds.columnsByName.mapValues(_.id.underlying)
    val rawSchema: Map[String, SoQLType] = ds.schemaSpec.schema.map { case (k, v) => (k.underlying, v) }

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

    val dsCtx = new DatasetContext[SoQLType] {
      val schema = OrderedMap(columnIdMap.mapValues(rawSchema).toSeq.sortBy(_._1) : _*)
    }
    try {
      val analysis = analyzer.analyzeUnchainedQuery(query)(Map(TableName.PrimaryTable.qualifier -> dsCtx))
      log.debug(s"Rollup analysis successful: ${analysis}")
      Right(analysis)
    } catch {
      case NoSuchColumn(name, _) => Left(RollupColumnNotFound(name))
      case e: SoQLException => Left(RollupError(e.getMessage))
    }
  }

  def getRollups(dataset: ResourceName, requestId: RequestId): Result = {
    store.lookupDataset(dataset, store.latestCopyNumber(dataset)) match {
      case Some(datasetRecord) =>
        val columnNameMap = datasetRecord.columns.map { columnRecord =>
          (rollupColumnNameToIdMapping(columnRecord.id), columnRecord.fieldName)
        }.toMap

        dc.getRollups(datasetRecord.systemId, traceHeaders(requestId, dataset)) match {
          case result: DataCoordinatorClient.RollupResult =>
            val rollups = result.rollups.map { rollup =>
              val parsedQuery = new StandaloneParser().selectStatement(rollup.soql)
              val mappedQueries = new ColumnNameMapper(columnNameMap).mapSelect(parsedQuery)

              mappedQueries.size match {
                case 1 =>
                  val mappedQuery = mappedQueries.head
                  log.debug(s"soql for rollup ${rollup} is: ${parsedQuery}")
                  log.debug(s"Mapped soql for rollup ${rollup} is: ${mappedQuery}")

                  RollupSpec(name = rollup.name, soql = mappedQuery.toString())
                case _ =>
                  log.error("found saved rollup to be chained soql")
                  return InternalServerError("unknown", tag, "found saved rollup to be chained soql")
              }
            }

            Rollups(rollups)
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            DatasetNotFound(dataset)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def deleteRollup(user: String, dataset: ResourceName, rollup: RollupName, requestId: RequestId): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        val instruction = DropRollupInstruction(rollup)

        dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user,
                  Iterator.single(instruction), traceHeaders(requestId, dataset)) {
          case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, lastModified) =>
            store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber, None)
            RollupDropped
          case DataCoordinatorClient.NoSuchRollupResult(_, _) =>
            RollupNotFound(rollup)
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            DatasetNotFound(dataset)
          case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
            CannotAcquireDatasetWriteLock(dataset)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def collocate(secondaryId: SecondaryId, operation: SFCollocateOperation, explain: Boolean, jobId: String): Result = {
    def translate(resource: ResourceName): Option[DatasetId] = store.translateResourceName(resource).map(_.systemId)
    DCCollocateOperation(operation, translate _) match {
      case Left(op) =>
        // TODO: Translate dc errors better, need to clarify what actually needs to be propogated
        dc.collocate(secondaryId, op, explain, jobId) match {
          case res: DataCoordinatorClient.CollocateResult =>
            CollocateDone(res, { datasetId => store.bulkDatasetLookup(Set(datasetId)).headOption })
          case DataCoordinatorClient.InstanceNotExistResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.StoreGroupNotExistResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.StoreNotExistResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.DatasetNotExistResult(e) => GenericCollocateError(e.underlying)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case Right(err) => err
    }
  }

  def collocateStatus(dataset: ResourceName, secondaryId: SecondaryId, jobId: String): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.collocateStatus(datasetRecord.systemId, secondaryId, jobId) match {
          case res: DataCoordinatorClient.CollocateResult =>
            CollocateDone(res, { datasetId => store.bulkDatasetLookup(Set(datasetId)).headOption })
          case DataCoordinatorClient.StoreGroupNotExistResult(e) => GenericCollocateError(e)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def deleteCollocate(dataset: ResourceName, secondaryId: SecondaryId, jobId: String): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.deleteCollocate(datasetRecord.systemId, secondaryId, jobId) match {
          case res: DataCoordinatorClient.CollocateResult =>
            CollocateDone(res, { datasetId => store.bulkDatasetLookup(Set(datasetId)).headOption })
          case DataCoordinatorClient.StoreGroupNotExistResult(e) => GenericCollocateError(e)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }
}

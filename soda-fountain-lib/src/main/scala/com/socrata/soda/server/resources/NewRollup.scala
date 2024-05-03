package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.JNull
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodec, OrJNull}

import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.TableName
import com.socrata.soql.stdlib.analyzer2.UserParameters

import com.socrata.soda.server.wiremodels.{InputUtils, NewRollupRequest}
import com.socrata.soda.server.wiremodels.metatypes.RollupMetaTypes
import com.socrata.soda.server.{SodaUtils, SodaRequest}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.copy.{Stage, Latest, Published}
import com.socrata.soda.server.util.{DAOCache, Lazy}
import com.socrata.soda.server.highlevel.RollupHelper
import com.socrata.soda.server.persistence.NameAndSchemaStore

import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, CreateOrUpdateRollupInstruction}

final abstract class NewRollup

case object NewRollup {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[NewRollup])

  type DCMT = DataCoordinatorClient.RollupMetaTypes

  @AutomaticJsonCodec
  case class NewRollupSoql(
    foundTables: UnparsedFoundTables[DCMT],
    locationSubcolumns: Map[
      types.DatabaseTableName[DCMT],
      Map[
        types.DatabaseColumnName[DCMT],
        Seq[Either[JNull, types.DatabaseColumnName[DCMT]]]
      ]
    ],
    userParameters: UserParameters,
    rewritePasses: Seq[Seq[rewrite.AnyPass]]
  )

  case object service extends SodaResource {
    override def post = doPost _

    private def doPost(req: SodaRequest): HttpResponse = {
      InputUtils.jsonSingleObjectStream(req.httpRequest, Long.MaxValue) match {
        case Right(obj) =>
          JsonDecode.fromJValue[NewRollupRequest](obj) match {
            case Right(reqData) =>
              log.debug("Received request {}", Lazy(JsonUtil.renderJson(reqData, pretty=true)))

              val tableNames = reqData.soql.allTableDescriptions.map { ds =>
                val DatabaseTableName(rn) = ds.name
                TableName(rn.name)
              }.toSet

              val cache = new DAOCache(req.nameAndSchemaStore)

              val currentDatasetName = reqData.baseDataset
              val currentDataset = req.nameAndSchemaStore.lookupDataset(currentDatasetName, Some(Latest)).getOrElse {
                // TODO: Better error
                return NotFound
              }

              trait RestagedMT extends MetaTypes {
                type DatabaseTableNameImpl = (ResourceName, Stage)

                type ResourceNameScope = RollupMetaTypes#ResourceNameScope
                type ColumnType = RollupMetaTypes#ColumnType
                type ColumnValue = RollupMetaTypes#ColumnValue
                type DatabaseColumnNameImpl = RollupMetaTypes#DatabaseColumnNameImpl
              }

              val staged = reqData.soql.rewriteDatabaseNames[RestagedMT](
                { case DatabaseTableName(name) =>
                  if(name == currentDatasetName) DatabaseTableName((name, currentDataset.stage.get)) // this ".get" annoys me!
                  else DatabaseTableName((name, Published))
                },
                { (_, dcn) => dcn }
              )

              val locationSubcolumns = cache.buildAuxTableData[RestagedMT](staged.allTableDescriptions)
                .getOrElse {
                  // TODO better error
                  return BadRequest
                }
                .iterator
                .map { case (dtn@DatabaseTableName((internalName, _)), value) =>
                  val DatabaseTableName((resourceName, _stage)) = cache.lookupResourceName(dtn).getOrElse {
                    // TODO better error
                    return BadRequest
                  }
                  DatabaseTableName(resourceName) -> value.locationSubcolumns
                }.toMap

              val dcFoundTables = staged.rewriteDatabaseNames[DataCoordinatorClient.RollupMetaTypes](
                { case dtn@DatabaseTableName((resourceName, _stage)) =>
                  cache.lookupTableName(dtn).getOrElse {
                    // TODO better error
                    return BadRequest
                  }
                  DatabaseTableName(resourceName)
                },
                cache.lookupColumnName(_, _).getOrElse {
                  // TODO better error
                  return BadRequest
                }
              )

              // Ok so from this point onward, there's no distinction
              // between old-rollups and new-rollups until they hit
              // the secondary.  This is a bit icky, it works because
              // '{' is not a valid initial character in an old-rollup...
              val datasetRecord = req.nameAndSchemaStore.translateResourceName(reqData.baseDataset).getOrElse {
                // TODO better error
                return BadRequest
              }

              val convertedSoql = NewRollupSoql(
                dcFoundTables,
                locationSubcolumns.mapValues(_.mapValues(_.map(_.toRight(JNull)))),
                reqData.userParameters,
                reqData.rewritePasses
              )

              val instruction = CreateOrUpdateRollupInstruction(reqData.name, JsonUtil.renderJson(convertedSoql, pretty = false), "")
              req.dataCoordinator.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion(req), user(req), Iterator.single(instruction)) {
                case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, newShapeVersion, lastModified) =>
                  req.nameAndSchemaStore.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber)
                  RollupHelper.rollupCreatedOrUpdated(req.nameAndSchemaStore, reqData.baseDataset, copyNumber, reqData.name, JsonUtil.renderJson(NewRollupRequest.Stored(reqData.soql, reqData.userParameters, reqData.rewritePasses), pretty = false), tableNames)
                  Created
                case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                  BadRequest // TODO better error
              }
            case Left(err) =>
              log.info("Body wasn't a NewRollupSpec: {}", err.english)
              BadRequest // todo: better error
          }
        case Left(err) =>
          SodaUtils.response(req, err)
      }
    }
  }
}

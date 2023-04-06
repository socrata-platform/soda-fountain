package com.socrata.soda.server.persistence

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{RollupResult, Result => DcResult}
import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, DataCoordinatorInstruction, RollupDatasetRelation, RollupInfo}
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.highlevel.DatasetDAO.{RollupMarkedAccessed, RollupRelations, RollupRelationsNotFound, Rollups, Result => DdResult}
import com.socrata.soda.server.id.{ColumnId, DatasetHandle, RollupName}
import com.socrata.soda.server.util.RelationSide
import com.socrata.soda.server.wiremodels.UserProvidedRollupSpec
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNumber, SoQLText, SoQLType}
import org.joda.time.DateTime
import org.scalatest.matchers.should.Matchers

class RollupPostgresStoreTest extends SodaPostgresContainerTest with Matchers {


  describe("Rollups") {

    describe("relations"){

      it("to") {
        val nameAndSchemaStore: NameAndSchemaStore = buildNameAndSchemaStore()
        val dcClient: DataCoordinatorClient = mock[DataCoordinatorClient]
        (dcClient.update(_: DatasetHandle, _: String, _: Option[Long], _: String, _: Iterator[DataCoordinatorInstruction])(_: DcResult => DdResult)).expects(
          *, *, *, *, *, *
        ).onCall((_, _, _, _, _, r) => r(DataCoordinatorClient.NonCreateScriptResult(
          null,
          None,
          1,
          0,
          0,
          DateTime.now()
        )))

        val datasetDao: DatasetDAO = buildDatasetDao(
          nameAndSchemaStore = nameAndSchemaStore,
          dataCoordinatorClient = dcClient
        )

        val dataset1 = generateDataset("_one1234", List(
          ColumnRecord(
            ColumnId("id"),
            ColumnName("id"),
            SoQLNumber.t,
            false,
            None
          ),
          ColumnRecord(
            ColumnId("name"),
            ColumnName("name"),
            SoQLText.t,
            false,
            None
          )
        ))
        nameAndSchemaStore.addResource(dataset1)

        val dataset2 = generateDataset("_two1234", List(
          ColumnRecord(
            ColumnId("person"),
            ColumnName("person"),
            SoQLNumber.t,
            false,
            None
          ),
          ColumnRecord(
            ColumnId("action"),
            ColumnName("action"),
            SoQLText.t,
            false,
            None
          )
        ))
        nameAndSchemaStore.addResource(dataset2)

        val rollupName = new RollupName("one");
        val soql = s"select `name`, count(@actions.`action`) join @${dataset2.resourceName.name.substring(1)} as @actions on `id` = @actions.`person` group by `name`"

        datasetDao.replaceOrCreateRollup(
          "user1",
          dataset1.resourceName,
          None,
          new RollupName("one"),
          UserProvidedRollupSpec(
            Some(rollupName),
            Some(soql)
          )
        )

        datasetDao.getRollupRelations(dataset1.resourceName,RelationSide.To)match{
          case RollupRelations(relations)=> relations should equal(
            Set(
              RollupDatasetRelation(
                dataset1.resourceName,
                rollupName,
                soql,
                Set(dataset2.resourceName)
              )
            )
          )
          case RollupRelationsNotFound() => fail("Expected relations")
        }
      }

      it("from") {
        val nameAndSchemaStore: NameAndSchemaStore = buildNameAndSchemaStore()
        val dcClient: DataCoordinatorClient = mock[DataCoordinatorClient]
        (dcClient.update(_: DatasetHandle, _: String, _: Option[Long], _: String, _: Iterator[DataCoordinatorInstruction])(_: DcResult => DdResult)).expects(
          *, *, *, *, *, *
        ).onCall((_, _, _, _, _, r) => r(DataCoordinatorClient.NonCreateScriptResult(
          null,
          None,
          1,
          0,
          0,
          DateTime.now()
        )))

        val datasetDao: DatasetDAO = buildDatasetDao(
          nameAndSchemaStore = nameAndSchemaStore,
          dataCoordinatorClient = dcClient
        )

        val dataset1 = generateDataset("_one1234", List(
          ColumnRecord(
            ColumnId("id"),
            ColumnName("id"),
            SoQLNumber.t,
            false,
            None
          ),
          ColumnRecord(
            ColumnId("name"),
            ColumnName("name"),
            SoQLText.t,
            false,
            None
          )
        ))
        nameAndSchemaStore.addResource(dataset1)

        val dataset2 = generateDataset("_two1234", List(
          ColumnRecord(
            ColumnId("person"),
            ColumnName("person"),
            SoQLNumber.t,
            false,
            None
          ),
          ColumnRecord(
            ColumnId("action"),
            ColumnName("action"),
            SoQLText.t,
            false,
            None
          )
        ))
        nameAndSchemaStore.addResource(dataset2)

        val rollupName = new RollupName("one");
        val soql = s"select `name`, count(@actions.`action`) join @${dataset2.resourceName.name.substring(1)} as @actions on `id` = @actions.`person` group by `name`"

        datasetDao.replaceOrCreateRollup(
          "user1",
          dataset1.resourceName,
          None,
          new RollupName("one"),
          UserProvidedRollupSpec(
            Some(rollupName),
            Some(soql)
          )
        )

        datasetDao.getRollupRelations(dataset2.resourceName, RelationSide.From) match {
          case RollupRelations(relations) => relations should equal(
            Set(
              RollupDatasetRelation(
                dataset1.resourceName,
                rollupName,
                soql,
                Set(dataset2.resourceName)
              )
            )
          )
          case RollupRelationsNotFound() => fail("Expected relations")
        }
      }
    }


    describe("last accessed") {
      it("should not be marked as accessed") {
        val nameAndSchemaStore: NameAndSchemaStore = buildNameAndSchemaStore()

        val datasetDao: DatasetDAO = buildDatasetDao(
          nameAndSchemaStore = nameAndSchemaStore
        )

        val dataset = generateDataset("one1234", Seq.empty[ColumnRecord])
        nameAndSchemaStore.addResource(dataset)

        datasetDao.markRollupAccessed(dataset.resourceName, new RollupName("one")) match {
          case RollupMarkedAccessed() => fail("Should have failed to mark rollup as accessed")
          case _ =>
        }
      }

      it("should be marked as accessed") {
        val nameAndSchemaStore: NameAndSchemaStore = buildNameAndSchemaStore()
        val dcClient: DataCoordinatorClient = mock[DataCoordinatorClient]
        (dcClient.update(_: DatasetHandle, _: String, _: Option[Long], _: String, _: Iterator[DataCoordinatorInstruction])(_: DcResult => DdResult)).expects(
          *, *, *, *, *, *
        ).onCall((_, _, _, _, _, r) => r(DataCoordinatorClient.NonCreateScriptResult(
          null,
          None,
          1,
          0,
          0,
          DateTime.now()
        )))

        val datasetDao: DatasetDAO = buildDatasetDao(
          nameAndSchemaStore = nameAndSchemaStore,
          dataCoordinatorClient = dcClient
        )

        val dataset = generateDataset("one1234", Seq.empty[ColumnRecord])
        nameAndSchemaStore.addResource(dataset)

        val x = datasetDao.replaceOrCreateRollup(
          "user1",
          dataset.resourceName,
          None,
          new RollupName("one"),
          UserProvidedRollupSpec(
            Some(new RollupName("one")),
            Some("select *")
          )
        )

        datasetDao.markRollupAccessed(dataset.resourceName, new RollupName("one")) match {
          case RollupMarkedAccessed() =>
          case _ => fail("Should have succeeded to mark rollup as accessed")
        }
      }

      it("should show as accessed") {
        val nameAndSchemaStore: NameAndSchemaStore = buildNameAndSchemaStore()
        val dcClient: DataCoordinatorClient = mock[DataCoordinatorClient]
        (dcClient.update(_: DatasetHandle, _: String, _: Option[Long], _: String, _: Iterator[DataCoordinatorInstruction])(_: DcResult => DdResult)).expects(
          *, *, *, *, *, *
        ).onCall((_, _, _, _, _, r) => r(DataCoordinatorClient.NonCreateScriptResult(
          null,
          None,
          1,
          0,
          0,
          DateTime.now()
        )))

        val datasetDao: DatasetDAO = buildDatasetDao(
          nameAndSchemaStore = nameAndSchemaStore,
          dataCoordinatorClient = dcClient
        )

        val dataset = generateDataset("one1234", Seq.empty[ColumnRecord])
        nameAndSchemaStore.addResource(dataset)

        val x = datasetDao.replaceOrCreateRollup(
          "user1",
          dataset.resourceName,
          None,
          new RollupName("one"),
          UserProvidedRollupSpec(
            Some(new RollupName("one")),
            Some("select *")
          )
        )

        datasetDao.markRollupAccessed(dataset.resourceName, new RollupName("one")) match {
          case RollupMarkedAccessed() =>
          case _ => fail("Should have succeeded to mark rollup as accessed")
        }

        (dcClient.getRollups(_: DatasetHandle)).expects(
          *
        ).returning(
          RollupResult(Seq.empty)
        )

        datasetDao.getRollups(dataset.resourceName) match {
          case Rollups(spec) => spec.filter(_.lastAccessed.isDefined) should have size (1)
          case _ => fail("Rollups should have been present")
        }
      }

      it("should show as accessed, with dc") {
        val nameAndSchemaStore: NameAndSchemaStore = buildNameAndSchemaStore()
        val dcClient: DataCoordinatorClient = mock[DataCoordinatorClient]
        (dcClient.update(_: DatasetHandle, _: String, _: Option[Long], _: String, _: Iterator[DataCoordinatorInstruction])(_: DcResult => DdResult)).expects(
          *, *, *, *, *, *
        ).onCall((_, _, _, _, _, r) => r(DataCoordinatorClient.NonCreateScriptResult(
          null,
          None,
          1,
          0,
          0,
          DateTime.now()
        )))

        val datasetDao: DatasetDAO = buildDatasetDao(
          nameAndSchemaStore = nameAndSchemaStore,
          dataCoordinatorClient = dcClient
        )

        val dataset = generateDataset("one1234", Seq.empty[ColumnRecord])
        nameAndSchemaStore.addResource(dataset)

        val x = datasetDao.replaceOrCreateRollup(
          "user1",
          dataset.resourceName,
          None,
          new RollupName("one"),
          UserProvidedRollupSpec(
            Some(new RollupName("one")),
            Some("select *")
          )
        )

        datasetDao.markRollupAccessed(dataset.resourceName, new RollupName("one")) match {
          case RollupMarkedAccessed() =>
          case _ => fail("Should have succeeded to mark rollup as accessed")
        }

        (dcClient.getRollups(_: DatasetHandle)).expects(
          *
        ).returning(
          RollupResult(List(
            RollupInfo(new RollupName("two"), "select *")
          ))
        )

        datasetDao.getRollups(dataset.resourceName) match {
          case Rollups(spec) => {
            //1 rollup spec from dc, without lastAccessed
            //1 rollup spec from soda, with lastAccessed
            spec should have size (2)
            spec.filter(_.lastAccessed.isDefined) should have size (1)
          }
          case _ => fail("Rollups should have been present")
        }
      }
    }
  }

}

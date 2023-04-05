package com.socrata.soda.server.persistence

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.{Result => DcResult}
import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, DataCoordinatorInstruction}
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.highlevel.DatasetDAO.{RollupMarkedAccessed, Result => DdResult}
import com.socrata.soda.server.id.{DatasetHandle, RollupName}
import com.socrata.soda.server.wiremodels.UserProvidedRollupSpec
import org.joda.time.DateTime
import org.scalatest.Matchers

class RollupPostgresStoreTest extends SodaPostgresContainerTest with Matchers {

  test("Should not mark rollup accessed if it does not exist") {
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

  test("Should mark rollup accessed if it exists") {
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

}

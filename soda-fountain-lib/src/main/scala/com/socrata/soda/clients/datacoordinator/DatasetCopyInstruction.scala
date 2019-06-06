package com.socrata.soda.clients.datacoordinator

import com.socrata.soda.server.id.ResourceName

sealed abstract class DatasetCopyInstruction { val command: String}

case class CreateDataset(resource: ResourceName, locale: String = "en_US") extends DatasetCopyInstruction { val command = "create"}
case class CopyDataset(copyData: Boolean, schema: String, expectedDataVersion: Option[Long]) extends DatasetCopyInstruction { val command = "copy"}
case class PublishDataset(keepSnapshot: Option[Boolean], schema: String, expectedDataVersion: Option[Long]) extends DatasetCopyInstruction { val command = "publish"}
case class DropDataset(schema: String, expectedDataVersion: Option[Long]) extends DatasetCopyInstruction { val command = "drop"}
case class UpdateDataset(schema: String, expectedDataVersion: Option[Long]) extends DatasetCopyInstruction { val command = "normal"}

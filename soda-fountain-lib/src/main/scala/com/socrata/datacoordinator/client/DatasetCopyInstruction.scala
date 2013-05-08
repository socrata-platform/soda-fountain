package com.socrata.datacoordinator.client

sealed abstract class DatasetCopyInstruction { val command: String}

case class CreateDataset(locale: String = "en_US") extends DatasetCopyInstruction { val command = "create"}
case class CopyDataset(copyData: Boolean, schema: Option[String]) extends DatasetCopyInstruction { val command = "copy"}
case class PublishDataset(snapshotLimit: Option[Int], schema: Option[String]) extends DatasetCopyInstruction { val command = "publish"}
case class DropDataset(schema: Option[String]) extends DatasetCopyInstruction { val command = "drop"}
case class UpdateDataset(schema: Option[String]) extends DatasetCopyInstruction { val command = "normal"}


package com.socrata.soda.clients.datacoordinator

sealed abstract class DatasetCopyInstruction { val command: String}

case class CreateDataset(locale: String = "en_US") extends DatasetCopyInstruction { val command = "create"}
case class CopyDataset(copyData: Boolean, schema: String) extends DatasetCopyInstruction { val command = "copy"}
case class PublishDataset(snapshotLimit: Option[Int], schema: String) extends DatasetCopyInstruction { val command = "publish"}
case class DropDataset(schema: String) extends DatasetCopyInstruction { val command = "drop"}
case class UpdateDataset(schema: String) extends DatasetCopyInstruction { val command = "normal"}

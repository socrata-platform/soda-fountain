package com.socrata.datacoordinator.client

object DatasetCopyInstruction extends Enumeration {
  type DatasetCopyInstruction = Value
  val create, copy, publish, drop, normal = Value
}

class DatasetCopyInstruction

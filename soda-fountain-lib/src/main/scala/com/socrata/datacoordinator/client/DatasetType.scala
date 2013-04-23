package com.socrata.datacoordinator.client

sealed abstract class DatasetType { val name: String}

case class Text() extends DatasetType { val name = "text" }
case class Number() extends DatasetType { val name = "number" }

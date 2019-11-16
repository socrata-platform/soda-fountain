package com.socrata.soda.message

import com.socrata.eurybates.Tag

object EurybatesMessage {
  val SecondaryDataVersionUpdated = "SECONDARY_DATA_VERSION_UPDATED"
  val SecondaryGroupDataVersionUpdated = "SECONDARY_GROUP_DATA_VERSION_UPDATED"

  def tag(message: Message): Tag = message match {
    case _: StoreReplicationComplete => SecondaryDataVersionUpdated
    case _: GroupReplicationComplete => SecondaryGroupDataVersionUpdated
  }
}

package com.socrata.soda.server.model

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.{CopyId, RollupMapId, RollupName}

import java.time.OffsetDateTime

case class RollupInfo(id: RollupMapId,copyId: CopyId,name: RollupName, soql: String, lastAccessed:OffsetDateTime)

object RollupInfo {
  import com.rojoma.json.v3.util.time.ISO8601.codec.offsetDateTimeCodec
  implicit val codec = AutomaticJsonCodecBuilder[RollupInfo]
}

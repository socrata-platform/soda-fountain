package com.socrata.soda.message


import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util._

case class ViewUid(uid: String)

object ViewUid {
  implicit object encode extends JsonEncode[ViewUid] {
    def encode(s: ViewUid) = JString(s.uid)
  }
}

object ToViewUid {
  // drop the leading '_' on resource_name to convert to view uid
  def apply(resourceName: String): ViewUid = ViewUid(resourceName.drop(1))
}

sealed abstract class Message

// We aren't using this message right now, but I have hopes to use it for
// webhooks or other things that want to know about when NBE replication
// is complete. So going to leave this here for now.
case class StoreReplicationComplete(viewUid: ViewUid,
                                    groupName: Option[String],
                                    storeId: String,
                                    newDataVersion: Long,
                                    startingAtMs: Long,
                                    endingAtMs: Long) extends Message

object StoreReplicationComplete {
  implicit val encode = AutomaticJsonEncodeBuilder[StoreReplicationComplete]
}

case class GroupReplicationComplete(viewUid: ViewUid,
                                    groupName: String,
                                    storeIds: Set[String],
                                    newDataVersion: Long,
                                    endingAtMs: Long) extends Message

object GroupReplicationComplete {
  implicit val encode = AutomaticJsonEncodeBuilder[GroupReplicationComplete]
}

object Message {
  implicit val encode = SimpleHierarchyEncodeBuilder[Message](NoTag)
    .branch[StoreReplicationComplete]
    .branch[GroupReplicationComplete]
    .build
}

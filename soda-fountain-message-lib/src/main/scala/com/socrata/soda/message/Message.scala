package com.socrata.soda.message


import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util._
import com.rojoma.json.v3.interpolation._

// Metric Message Sample
/***
{
  "entityId" : "nn5w-zj56",
  "metrics" : {
    "rows-accessed-website" : {
      "type" : "aggregate",
      "value" : 2
    },
    "bytes-out" : {
      "type" : "aggregate",
      "value" : 16228
    },
    "bytes-in" : {
      "type" : "aggregate",
      "value" : 3554
    },
    "view-loaded" : {
      "type" : "aggregate",
      "value" : 1
    },
    "rows-loaded-website" : {
      "type" : "aggregate",
      "value" : 246
    }
  },
  "timestamp" : 1573965000000
}

 ***/

case class RowsLoadedApiMetricMessage(resourceName: String, v: Long) extends MetricMessage(resourceName, "rows-loaded-api", v, System.currentTimeMillis())

class MetricMessage(val entityId: String, val name: String, val value: Long, val timeMs: Long) extends Message

object MetricMessage {
  implicit object MetricMessageJsonEncode extends JsonEncode[MetricMessage] {
    def encode(mm: MetricMessage): JObject = {
      json""" { "entityId": ${mm.entityId},
                "metrics": {
                             ${mm.name} : { "type": "aggregate", "value": ${mm.value} }
                           },
                "timestamp" : ${mm.timeMs}
              }"""
    }
  }
}


sealed abstract class Message

object Message {
  implicit val encode = SimpleHierarchyEncodeBuilder[Message](NoTag)
    .branch[MetricMessage]
    .build
}

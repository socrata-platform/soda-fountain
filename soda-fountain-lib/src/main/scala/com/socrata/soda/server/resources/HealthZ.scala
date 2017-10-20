package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.JObject
//import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

object HealthZ {

  object service extends SodaResource {
    override def get = { req => response =>
      // TODO: do we still even want this endpoint now that soda-fountain nolonger depends on region coder?
      (OK ~> Json(JObject.canonicalEmpty))(response)
    }
  }
}

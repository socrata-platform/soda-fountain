package com.socrata.soda.server.resources

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.SodaUtils
import javax.servlet.http.HttpServletRequest

object HealthZ {
  val log = org.slf4j.LoggerFactory.getLogger(HealthZ.getClass)

  object service extends SodaResource {

    override def get = { req: HttpServletRequest =>
      OK ~> SodaUtils.JsonContent("TODO")
    }
  }
}

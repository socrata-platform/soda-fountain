package com.socrata.soda.server.resources

import com.rojoma.json.io.JsonReader
import com.rojoma.simplearm.util._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.SodaUtils
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import javax.servlet.http.HttpServletRequest

final abstract class Version

object Version {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Version])

  object service extends SodaResource {
    val responseJ = for {
      stream <- managed(getClass.getClassLoader.getResourceAsStream("soda-fountain-version.json"))
      reader <- managed(new InputStreamReader(stream, StandardCharsets.UTF_8))
    } yield JsonReader.fromReader(reader)

    def response = {
      log.info("TODO: Negotiate content-type")
      OK ~> SodaUtils.JsonContent(responseJ)
    }

    override val get = (_: HttpServletRequest) => response
  }
}

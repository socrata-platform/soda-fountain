package com.socrata.soda.server.resources

import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.server.SodaUtils
import com.rojoma.simplearm.util._

object VersionService extends SodaResource {
  val responseString = for {
    stream <- managed(getClass.getClassLoader.getResourceAsStream("soda-fountain-version.json"))
    source <- managed(scala.io.Source.fromInputStream(stream)(scala.io.Codec.UTF8))
  } yield source.mkString

  val response =
    OK ~> ContentType(SodaUtils.jsonContentType) ~> Content(responseString)

  override val get = (_: HttpServletRequest) => response
}

package com.socrata.soda.server

import com.socrata.http.routing._
import com.socrata.http.routing.HttpMethods._
import com.socrata.http.server._
import javax.servlet.http._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some
import com.socrata.soda.server.services._

trait SodaRouter extends SodaFountain {

  val router = RouterSet(

    ExtractingRouter[HttpService](POST,   "/resource/?")        ( dataset.upsert _),
    ExtractingRouter[HttpService](PUT,    "/resource/?")        ( dataset.replace _),
    ExtractingRouter[HttpService](DELETE, "/resource/?")        ( dataset.truncate _),
    ExtractingRouter[HttpService](GET,    "/resource/?")        ( dataset.query _),

    ExtractingRouter[HttpService](POST,   "/resource/?/?")        ( rows.upsert _),
    ExtractingRouter[HttpService](PUT,    "/resource/?/?")        ( notSupported2 _),
    ExtractingRouter[HttpService](DELETE, "/resource/?/?")        ( rows.delete _),
    ExtractingRouter[HttpService](GET,    "/resource/?/?")        ( rows.get _),

    ExtractingRouter[HttpService](POST,   "/dataset")          ( dataset.create _),
    ExtractingRouter[HttpService](POST,   "/dataset/?")        ( dataset.setSchema _),
    ExtractingRouter[HttpService](PUT,    "/dataset/?")        ( notSupported _),
    ExtractingRouter[HttpService](DELETE, "/dataset/?")        ( dataset.delete _),
    ExtractingRouter[HttpService](GET,    "/dataset/?")        ( dataset.getSchema _),

    ExtractingRouter[HttpService](GET,    "/dataset-copy/?")        ( dataset.copy _),
    ExtractingRouter[HttpService](DELETE, "/dataset-copy/?")        ( dataset.dropCopy _),
    ExtractingRouter[HttpService](PUT,    "/dataset-copy/?")        ( dataset.publish _),

    ExtractingRouter[HttpService](POST,   "/dataset/?/?")        ( columns.update _),
    ExtractingRouter[HttpService](PUT,    "/dataset/?/?")        ( notSupported2 _),
    ExtractingRouter[HttpService](DELETE, "/dataset/?/?")        ( columns.drop _),
    ExtractingRouter[HttpService](GET,    "/dataset/?/?")        ( columns.getSchema _)
  )
  def route(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }
}

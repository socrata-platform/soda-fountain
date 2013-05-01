package com.socrata.soda.server

import com.socrata.http.routing._
import com.socrata.http.routing.HttpMethods._
import com.socrata.http.server._
import javax.servlet.http._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some
import com.socrata.soda.server.services._

class SodaRouter(data: DatasetService, columns: ColumnService, catalog: CatalogService) {

  private val router = RouterSet(
    ExtractingRouter[HttpService](GET, "/resource/?/data")( data.query _),
    ExtractingRouter[HttpService](GET, "/resource/?/data/?")( data.get _),
    ExtractingRouter[HttpService](POST, "/resource/?/data/?")( data.set _),
    ExtractingRouter[HttpService](POST, "/resource/?")( data.setRowFromPost _),
    ExtractingRouter[HttpService](DELETE, "/resource/?/data/?")( data.delete _),

    ExtractingRouter[HttpService](POST, "/resource/?/columns")( columns.create _),
    ExtractingRouter[HttpService](GET, "/resource/?/columns")( columns.getAll _),
    ExtractingRouter[HttpService](PUT, "/resource/?/column/?")( columns.set _),
    ExtractingRouter[HttpService](DELETE, "/resource/?/column/?")( columns.delete _),
    ExtractingRouter[HttpService](GET, "/resource/?/column/?")( columns.get _),

    ExtractingRouter[HttpService](POST, "/resources")( catalog.create _),
    ExtractingRouter[HttpService](DELETE, "/resource/?")( catalog.delete _),
    ExtractingRouter[HttpService](PUT, "/resource/?")( catalog.set _),
    ExtractingRouter[HttpService](GET, "/resource/?")( catalog.get _)

  )
  def route(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }
}

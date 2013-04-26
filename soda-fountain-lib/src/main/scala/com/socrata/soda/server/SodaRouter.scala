package com.socrata.soda.server

import com.socrata.http.routing._
import com.socrata.http.routing.HttpMethods._
import com.socrata.http.server._
import javax.servlet.http._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import scala.Some
import com.socrata.soda.server.services._

object SodaRouter {

  private val router = RouterSet(
    ExtractingRouter[HttpService](GET, "/resource/?/data")( DatasetService.query _),
    ExtractingRouter[HttpService](GET, "/resource/?/data/?")( DatasetService.get _),
    ExtractingRouter[HttpService](POST, "/resource/?/data/?")( DatasetService.set _),
    ExtractingRouter[HttpService](POST, "/resource/?")( DatasetService.setRowFromPost _),
    ExtractingRouter[HttpService](DELETE, "/resource/?/data/?")( DatasetService.delete _),

    ExtractingRouter[HttpService](POST, "/resource/?/columns")( ColumnService.create _),
    ExtractingRouter[HttpService](GET, "/resource/?/columns")( ColumnService.getAll _),
    ExtractingRouter[HttpService](PUT, "/resource/?/column/?")( ColumnService.set _),
    ExtractingRouter[HttpService](DELETE, "/resource/?/column/?")( ColumnService.delete _),
    ExtractingRouter[HttpService](GET, "/resource/?/column/?")( ColumnService.get _),

    ExtractingRouter[HttpService](POST, "/resources")( CatalogService.create _),
    ExtractingRouter[HttpService](DELETE, "/resource/?")( CatalogService.delete _),
    ExtractingRouter[HttpService](PUT, "/resource/?")( CatalogService.set _),
    ExtractingRouter[HttpService](GET, "/resource/?")( CatalogService.get _)

  )
  def routedService(req: HttpServletRequest): HttpResponse =
    router(req.getMethod, req.getRequestURI.split('/').tail) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
    }
}

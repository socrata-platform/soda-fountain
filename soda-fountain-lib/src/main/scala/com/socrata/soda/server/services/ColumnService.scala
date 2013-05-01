package com.socrata.soda.server.services

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

trait ColumnService {

  def getAll(datasetResourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def get(datasetResourceName: String, columnName:String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def set(datasetResourceName: String, columnName:String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def create(columnName:String)(request:HttpServletRequest): HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("create request not implemented")
  }

  def delete(datasetResourceName: String, columnName:String)(request:HttpServletRequest): HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("delete request not implemented")
  }

}

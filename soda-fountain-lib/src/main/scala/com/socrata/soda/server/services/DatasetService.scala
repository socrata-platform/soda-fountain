package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client.{UpsertRowInstruction, UpdateDataset, MutationScript, DataCoordinatorClient}
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil


object DatasetService {

  def query(datasetResourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def get(datasetResourceName: String, rowId:String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def setRowFromPost(datasetResourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    val fields = JsonUtil.readJson[Map[String,JValue]](request.getReader)
    fields match {
      case Some(map) => {
        val client = new DataCoordinatorClient("localhost:12345")
        val script = new MutationScript(datasetResourceName,
          "soda-fountain-community-edition",
          UpdateDataset(),
          Array(Right(UpsertRowInstruction(map))).toIterable)
        val response = client.sendMutateRequest(script)
        response() match {
          case Left(th) => {
            BadRequest ~> ContentType("text/plain; charset=utf-8") ~> Content(th.getMessage)
          }
          case Right(resp) => OK ~>  ContentType("text/plain; charset=utf-8") ~> Content(resp.getResponseBody)
        }
      }
      case None => BadRequest ~> ContentType("text/plain; charset=utf-8") ~> Content("bad request")
    }
  }

  def set(datasetResourceName: String, rowId:String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def create(datasetResourceName: String, rowId:String)(request:HttpServletRequest): HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("create request not implemented")
  }

  def delete(datasetResourceName: String, rowId:String)(request:HttpServletRequest): HttpServletResponse => Unit = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("delete request not implemented")
  }
}


package com.socrata.soda.server.wiremodels

import com.rojoma.json.ast.JObject
import com.socrata.soda.server.wiremodels.InputUtils._
import javax.servlet.http.HttpServletRequest

trait UserProvidedSpec[T] {
  def fromRequest(request: HttpServletRequest, approxLimit: Long): ExtractResult[T] =
    catchingInputProblems {
      jsonSingleObjectStream(request, approxLimit) match {
        case Right(jobj) => fromObject(jobj)
        case Left(err) => RequestProblem(err)
      }
    }

  def fromObject(obj: JObject): ExtractResult[T]
}
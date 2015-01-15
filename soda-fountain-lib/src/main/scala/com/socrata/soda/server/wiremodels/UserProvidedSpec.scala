package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast.JObject
import com.socrata.http.server.HttpRequest
import com.socrata.soda.server.wiremodels.InputUtils._

trait UserProvidedSpec[T] {
  def fromRequest(request: HttpRequest, approxLimit: Long): ExtractResult[T] =
    catchingInputProblems {
      jsonSingleObjectStream(request, approxLimit) match {
        case Right(jobj) => fromObject(jobj)
        case Left(err) => RequestProblem(err)
      }
    }

  def fromObject(obj: JObject): ExtractResult[T]
}
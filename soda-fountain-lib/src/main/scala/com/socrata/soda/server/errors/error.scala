package com.socrata.soda.server.errors

import javax.servlet.http.HttpServletResponse._
import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec

case class GeneralNotFoundError(path: String)
  extends SodaError(SC_NOT_FOUND, "not-found", "path" -> JString(path))

case class InternalError(tag: String)
  extends SodaError(SC_INTERNAL_SERVER_ERROR, "internal-error",
    "tag" -> JString(tag))

case class HttpMethodNotAllowed(method: String, allowed: TraversableOnce[String])
  extends SodaError(SC_METHOD_NOT_ALLOWED, "method-not-allowed",
    "method" -> JString(method),
    "allowed" -> JsonCodec.toJValue(allowed.toSeq))

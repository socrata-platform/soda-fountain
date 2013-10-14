package com.socrata.soda.server.util

import com.socrata.http.server.HttpService
import javax.servlet.http.HttpServletRequest

class ThreadNamingHandler(underlying: HttpService) extends HttpService {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[LoggingHandler])

  def apply(req: HttpServletRequest) = { resp =>
    val oldName = Thread.currentThread.getName
    try {
      Thread.currentThread.setName(Thread.currentThread.getId + " / " + req.getMethod + " " + req.getRequestURI)
      underlying(req)(resp)
    } finally {
      Thread.currentThread.setName(oldName)
    }
  }
}

object ThreadNamingHandler extends (HttpService => HttpService) {
  def apply(service: HttpService): ThreadNamingHandler = new ThreadNamingHandler(service)
}

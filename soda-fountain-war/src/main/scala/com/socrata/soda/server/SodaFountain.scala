package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServlet, HttpServletRequest}

class SodaFountain extends HttpServlet {
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) = 
    resp.getWriter().print("well this is working")
}

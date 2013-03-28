import javax.servlet.http._

class SodaServer extends HttpServlet {
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) = 
    resp.getWriter().print("well this is working")
}

object SodaServer {
  def main(args: Array[String]) = println("well this is working")
}

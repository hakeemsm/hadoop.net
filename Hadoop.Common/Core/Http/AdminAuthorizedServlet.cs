using Javax.Servlet.Http;
using Org.Mortbay.Jetty.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>General servlet which is admin-authorized.</summary>
	[System.Serializable]
	public class AdminAuthorizedServlet : DefaultServlet
	{
		private const long serialVersionUID = 1L;

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			// Do the authorization
			if (HttpServer2.HasAdministratorAccess(GetServletContext(), request, response))
			{
				// Authorization is done. Just call super.
				base.DoGet(request, response);
			}
		}
	}
}

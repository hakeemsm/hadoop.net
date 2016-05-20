using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>General servlet which is admin-authorized.</summary>
	[System.Serializable]
	public class AdminAuthorizedServlet : org.mortbay.jetty.servlet.DefaultServlet
	{
		private const long serialVersionUID = 1L;

		/// <exception cref="javax.servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			// Do the authorization
			if (org.apache.hadoop.http.HttpServer2.hasAdministratorAccess(getServletContext()
				, request, response))
			{
				// Authorization is done. Just call super.
				base.doGet(request, response);
			}
		}
	}
}

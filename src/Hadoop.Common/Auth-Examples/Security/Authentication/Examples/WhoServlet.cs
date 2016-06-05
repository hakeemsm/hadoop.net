using System.IO;
using Javax.Servlet.Http;


namespace Org.Apache.Hadoop.Security.Authentication.Examples
{
	/// <summary>Example servlet that returns the user and principal of the request.</summary>
	[System.Serializable]
	public class WhoServlet : HttpServlet
	{
		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
		{
			resp.SetContentType("text/plain");
			resp.SetStatus(HttpServletResponse.ScOk);
			string user = req.GetRemoteUser();
			string principal = (req.GetUserPrincipal() != null) ? req.GetUserPrincipal().GetName
				() : null;
			TextWriter writer = resp.GetWriter();
			writer.Write(MessageFormat.Format("You are: user[{0}] principal[{1}]\n", user, principal
				));
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoPost(HttpServletRequest req, HttpServletResponse resp)
		{
			DoGet(req, resp);
		}
	}
}

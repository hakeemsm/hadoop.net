using Sharpen;

namespace org.apache.hadoop.security.authentication.examples
{
	/// <summary>Example servlet that returns the user and principal of the request.</summary>
	[System.Serializable]
	public class WhoServlet : javax.servlet.http.HttpServlet
	{
		/// <exception cref="javax.servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
			 resp)
		{
			resp.setContentType("text/plain");
			resp.setStatus(javax.servlet.http.HttpServletResponse.SC_OK);
			string user = req.getRemoteUser();
			string principal = (req.getUserPrincipal() != null) ? req.getUserPrincipal().getName
				() : null;
			System.IO.TextWriter writer = resp.getWriter();
			writer.write(java.text.MessageFormat.format("You are: user[{0}] principal[{1}]\n"
				, user, principal));
		}

		/// <exception cref="javax.servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void doPost(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
			 resp)
		{
			doGet(req, resp);
		}
	}
}

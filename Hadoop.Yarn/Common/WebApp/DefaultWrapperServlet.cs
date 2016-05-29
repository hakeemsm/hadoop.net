using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	[System.Serializable]
	public class DefaultWrapperServlet : HttpServlet
	{
		private const long serialVersionUID = 1L;

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
		{
			RequestDispatcher rd = GetServletContext().GetNamedDispatcher("default");
			HttpServletRequest wrapped = new _HttpServletRequestWrapper_46(req);
			rd.Forward(wrapped, resp);
		}

		private sealed class _HttpServletRequestWrapper_46 : HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_46(HttpServletRequest baseArg1)
				: base(baseArg1)
			{
			}

			public override string GetServletPath()
			{
				return string.Empty;
			}
		}
	}
}

using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>Servlet utility methods.</summary>
	internal class ServletUtils
	{
		private static readonly java.nio.charset.Charset UTF8_CHARSET = java.nio.charset.Charset
			.forName("UTF-8");

		/// <summary>
		/// Extract a query string parameter without triggering http parameters
		/// processing by the servlet container.
		/// </summary>
		/// <param name="request">the request</param>
		/// <param name="name">the parameter to get the value.</param>
		/// <returns>
		/// the parameter value, or <code>NULL</code> if the parameter is not
		/// defined.
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if there was an error parsing the query string.
		/// 	</exception>
		public static string getParameter(javax.servlet.http.HttpServletRequest request, 
			string name)
		{
			System.Collections.Generic.IList<org.apache.http.NameValuePair> list = org.apache.http.client.utils.URLEncodedUtils
				.parse(request.getQueryString(), UTF8_CHARSET);
			if (list != null)
			{
				foreach (org.apache.http.NameValuePair nv in list)
				{
					if (name.Equals(nv.getName()))
					{
						return nv.getValue();
					}
				}
			}
			return null;
		}
	}
}

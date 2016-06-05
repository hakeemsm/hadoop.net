using System.Collections.Generic;
using System.Text;
using Javax.Servlet.Http;
using Org.Apache.Http;
using Org.Apache.Http.Client.Utils;


namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>Servlet utility methods.</summary>
	internal class ServletUtils
	{
		private static readonly Encoding Utf8Charset = Extensions.GetEncoding("UTF-8"
			);

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
		public static string GetParameter(HttpServletRequest request, string name)
		{
			IList<NameValuePair> list = URLEncodedUtils.Parse(request.GetQueryString(), Utf8Charset
				);
			if (list != null)
			{
				foreach (NameValuePair nv in list)
				{
					if (name.Equals(nv.GetName()))
					{
						return nv.GetValue();
					}
				}
			}
			return null;
		}
	}
}

using Sharpen;

namespace org.apache.hadoop.util
{
	public class ServletUtil
	{
		/// <summary>Initial HTML header</summary>
		/// <exception cref="System.IO.IOException"/>
		public static java.io.PrintWriter initHTML(javax.servlet.ServletResponse response
			, string title)
		{
			response.setContentType("text/html");
			java.io.PrintWriter @out = response.getWriter();
			@out.println("<html>\n" + "<link rel='stylesheet' type='text/css' href='/static/hadoop.css'>\n"
				 + "<title>" + title + "</title>\n" + "<body>\n" + "<h1>" + title + "</h1>\n");
			return @out;
		}

		/// <summary>Get a parameter from a ServletRequest.</summary>
		/// <remarks>
		/// Get a parameter from a ServletRequest.
		/// Return null if the parameter contains only white spaces.
		/// </remarks>
		public static string getParameter(javax.servlet.ServletRequest request, string name
			)
		{
			string s = request.getParameter(name);
			if (s == null)
			{
				return null;
			}
			s = s.Trim();
			return s.Length == 0 ? null : s;
		}

		/// <returns>
		/// a long value as passed in the given parameter, throwing
		/// an exception if it is not present or if it is not a valid number.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static long parseLongParam(javax.servlet.ServletRequest request, string param
			)
		{
			string paramStr = request.getParameter(param);
			if (paramStr == null)
			{
				throw new System.IO.IOException("Invalid request has no " + param + " parameter");
			}
			return long.Parse(paramStr);
		}

		public static readonly string HTML_TAIL = "<hr />\n" + "<a href='http://hadoop.apache.org/core'>Hadoop</a>, "
			 + java.util.Calendar.getInstance().get(java.util.Calendar.YEAR) + ".\n" + "</body></html>";

		/// <summary>HTML footer to be added in the jsps.</summary>
		/// <returns>the HTML footer.</returns>
		public static string htmlFooter()
		{
			return HTML_TAIL;
		}

		/// <summary>Escape and encode a string regarded as within the query component of an URI.
		/// 	</summary>
		/// <param name="value">the value to encode</param>
		/// <returns>encoded query, null if the default charset is not supported</returns>
		public static string encodeQueryValue(string value)
		{
			try
			{
				return org.apache.commons.httpclient.util.URIUtil.encodeWithinQuery(value, "UTF-8"
					);
			}
			catch (org.apache.commons.httpclient.URIException)
			{
				throw new java.lang.AssertionError("JVM does not support UTF-8");
			}
		}

		// should never happen!
		/// <summary>Escape and encode a string regarded as the path component of an URI.</summary>
		/// <param name="path">the path component to encode</param>
		/// <returns>encoded path, null if UTF-8 is not supported</returns>
		public static string encodePath(string path)
		{
			try
			{
				return org.apache.commons.httpclient.util.URIUtil.encodePath(path, "UTF-8");
			}
			catch (org.apache.commons.httpclient.URIException)
			{
				throw new java.lang.AssertionError("JVM does not support UTF-8");
			}
		}

		// should never happen!
		/// <summary>Parse and decode the path component from the given request.</summary>
		/// <param name="request">Http request to parse</param>
		/// <param name="servletName">the name of servlet that precedes the path</param>
		/// <returns>decoded path component, null if UTF-8 is not supported</returns>
		public static string getDecodedPath(javax.servlet.http.HttpServletRequest request
			, string servletName)
		{
			try
			{
				return org.apache.commons.httpclient.util.URIUtil.decode(getRawPath(request, servletName
					), "UTF-8");
			}
			catch (org.apache.commons.httpclient.URIException)
			{
				throw new java.lang.AssertionError("JVM does not support UTF-8");
			}
		}

		// should never happen!
		/// <summary>Parse the path component from the given request and return w/o decoding.
		/// 	</summary>
		/// <param name="request">Http request to parse</param>
		/// <param name="servletName">the name of servlet that precedes the path</param>
		/// <returns>path component, null if the default charset is not supported</returns>
		public static string getRawPath(javax.servlet.http.HttpServletRequest request, string
			 servletName)
		{
			com.google.common.@base.Preconditions.checkArgument(request.getRequestURI().StartsWith
				(servletName + "/"));
			return Sharpen.Runtime.substring(request.getRequestURI(), servletName.Length);
		}
	}
}

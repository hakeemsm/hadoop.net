using System;
using System.IO;
using Com.Google.Common.Base;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Httpclient;
using Org.Apache.Commons.Httpclient.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class ServletUtil
	{
		/// <summary>Initial HTML header</summary>
		/// <exception cref="System.IO.IOException"/>
		public static PrintWriter InitHTML(ServletResponse response, string title)
		{
			response.SetContentType("text/html");
			PrintWriter @out = response.GetWriter();
			@out.WriteLine("<html>\n" + "<link rel='stylesheet' type='text/css' href='/static/hadoop.css'>\n"
				 + "<title>" + title + "</title>\n" + "<body>\n" + "<h1>" + title + "</h1>\n");
			return @out;
		}

		/// <summary>Get a parameter from a ServletRequest.</summary>
		/// <remarks>
		/// Get a parameter from a ServletRequest.
		/// Return null if the parameter contains only white spaces.
		/// </remarks>
		public static string GetParameter(ServletRequest request, string name)
		{
			string s = request.GetParameter(name);
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
		public static long ParseLongParam(ServletRequest request, string param)
		{
			string paramStr = request.GetParameter(param);
			if (paramStr == null)
			{
				throw new IOException("Invalid request has no " + param + " parameter");
			}
			return long.Parse(paramStr);
		}

		public static readonly string HtmlTail = "<hr />\n" + "<a href='http://hadoop.apache.org/core'>Hadoop</a>, "
			 + Calendar.GetInstance().Get(Calendar.Year) + ".\n" + "</body></html>";

		/// <summary>HTML footer to be added in the jsps.</summary>
		/// <returns>the HTML footer.</returns>
		public static string HtmlFooter()
		{
			return HtmlTail;
		}

		/// <summary>Escape and encode a string regarded as within the query component of an URI.
		/// 	</summary>
		/// <param name="value">the value to encode</param>
		/// <returns>encoded query, null if the default charset is not supported</returns>
		public static string EncodeQueryValue(string value)
		{
			try
			{
				return URIUtil.EncodeWithinQuery(value, "UTF-8");
			}
			catch (URIException)
			{
				throw new Exception("JVM does not support UTF-8");
			}
		}

		// should never happen!
		/// <summary>Escape and encode a string regarded as the path component of an URI.</summary>
		/// <param name="path">the path component to encode</param>
		/// <returns>encoded path, null if UTF-8 is not supported</returns>
		public static string EncodePath(string path)
		{
			try
			{
				return URIUtil.EncodePath(path, "UTF-8");
			}
			catch (URIException)
			{
				throw new Exception("JVM does not support UTF-8");
			}
		}

		// should never happen!
		/// <summary>Parse and decode the path component from the given request.</summary>
		/// <param name="request">Http request to parse</param>
		/// <param name="servletName">the name of servlet that precedes the path</param>
		/// <returns>decoded path component, null if UTF-8 is not supported</returns>
		public static string GetDecodedPath(HttpServletRequest request, string servletName
			)
		{
			try
			{
				return URIUtil.Decode(GetRawPath(request, servletName), "UTF-8");
			}
			catch (URIException)
			{
				throw new Exception("JVM does not support UTF-8");
			}
		}

		// should never happen!
		/// <summary>Parse the path component from the given request and return w/o decoding.
		/// 	</summary>
		/// <param name="request">Http request to parse</param>
		/// <param name="servletName">the name of servlet that precedes the path</param>
		/// <returns>path component, null if the default charset is not supported</returns>
		public static string GetRawPath(HttpServletRequest request, string servletName)
		{
			Preconditions.CheckArgument(request.GetRequestURI().StartsWith(servletName + "/")
				);
			return Sharpen.Runtime.Substring(request.GetRequestURI(), servletName.Length);
		}
	}
}

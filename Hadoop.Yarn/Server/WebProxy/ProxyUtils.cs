/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	/// <summary>Class containing general purpose proxy utilities</summary>
	public class ProxyUtils
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(ProxyUtils));

		public const string EHttpHttpsOnly = "This filter only works for HTTP/HTTPS";

		public const string Location = "Location";

		public class _ : HamletSpec._
		{
			//Empty
		}

		public class Page : Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
		{
			internal Page(PrintWriter @out)
				: base(@out, 0, false)
			{
			}

			public virtual Hamlet.HTML<ProxyUtils._> Html()
			{
				return new Hamlet.HTML<ProxyUtils._>(this, "html", null, EnumSet.Of(HamletImpl.EOpt
					.Endtag));
			}
		}

		/// <summary>
		/// Handle redirects with a status code that can in future support verbs other
		/// than GET, thus supporting full REST functionality.
		/// </summary>
		/// <remarks>
		/// Handle redirects with a status code that can in future support verbs other
		/// than GET, thus supporting full REST functionality.
		/// <p>
		/// The target URL is included in the redirect text returned
		/// <p>
		/// At the end of this method, the output stream is closed.
		/// </remarks>
		/// <param name="request">
		/// request (hence: the verb and any other information
		/// relevant to a redirect)
		/// </param>
		/// <param name="response">the response</param>
		/// <param name="target">the target URL -unencoded</param>
		/// <exception cref="System.IO.IOException"/>
		public static void SendRedirect(HttpServletRequest request, HttpServletResponse response
			, string target)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Redirecting {} {} to {}", request.GetMethod(), request.GetRequestURI()
					, target);
			}
			string location = response.EncodeRedirectURL(target);
			response.SetStatus(HttpServletResponse.ScFound);
			response.SetHeader(Location, location);
			response.SetContentType(MimeType.Html);
			PrintWriter writer = response.GetWriter();
			ProxyUtils.Page p = new ProxyUtils.Page(writer);
			p.Html().Head().Title("Moved").().Body().H1("Moved").Div().("Content has moved ")
				.A(location, "here").().().();
			writer.Close();
		}

		/// <summary>Output 404 with appropriate message.</summary>
		/// <param name="resp">the http response.</param>
		/// <param name="message">the message to include on the page.</param>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		public static void NotFound(HttpServletResponse resp, string message)
		{
			resp.SetStatus(HttpServletResponse.ScNotFound);
			resp.SetContentType(MimeType.Html);
			ProxyUtils.Page p = new ProxyUtils.Page(resp.GetWriter());
			p.Html().H1(message).();
		}

		/// <summary>Reject any request that isn't from an HTTP servlet</summary>
		/// <param name="req">request</param>
		/// <exception cref="Javax.Servlet.ServletException">if the request is of the wrong type
		/// 	</exception>
		public static void RejectNonHttpRequests(ServletRequest req)
		{
			if (!(req is HttpServletRequest))
			{
				throw new ServletException(EHttpHttpsOnly);
			}
		}
	}
}

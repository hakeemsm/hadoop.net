using Sharpen;

namespace org.apache.hadoop.security.authentication.examples
{
	/// <summary>Servlet filter that logs HTTP request/response headers</summary>
	public class RequestLoggerFilter : javax.servlet.Filter
	{
		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.examples.RequestLoggerFilter))
			);

		/// <exception cref="javax.servlet.ServletException"/>
		public virtual void init(javax.servlet.FilterConfig filterConfig)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="javax.servlet.ServletException"/>
		public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
			 response, javax.servlet.FilterChain filterChain)
		{
			if (!LOG.isDebugEnabled())
			{
				filterChain.doFilter(request, response);
			}
			else
			{
				org.apache.hadoop.security.authentication.examples.RequestLoggerFilter.XHttpServletRequest
					 xRequest = new org.apache.hadoop.security.authentication.examples.RequestLoggerFilter.XHttpServletRequest
					((javax.servlet.http.HttpServletRequest)request);
				org.apache.hadoop.security.authentication.examples.RequestLoggerFilter.XHttpServletResponse
					 xResponse = new org.apache.hadoop.security.authentication.examples.RequestLoggerFilter.XHttpServletResponse
					((javax.servlet.http.HttpServletResponse)response);
				try
				{
					LOG.debug(xRequest.getResquestInfo().ToString());
					filterChain.doFilter(xRequest, xResponse);
				}
				finally
				{
					LOG.debug(xResponse.getResponseInfo().ToString());
				}
			}
		}

		public virtual void destroy()
		{
		}

		private class XHttpServletRequest : javax.servlet.http.HttpServletRequestWrapper
		{
			public XHttpServletRequest(javax.servlet.http.HttpServletRequest request)
				: base(request)
			{
			}

			public virtual System.Text.StringBuilder getResquestInfo()
			{
				System.Text.StringBuilder sb = new System.Text.StringBuilder(512);
				sb.Append("\n").Append("> ").Append(getMethod()).Append(" ").Append(getRequestURL
					());
				if (getQueryString() != null)
				{
					sb.Append("?").Append(getQueryString());
				}
				sb.Append("\n");
				System.Collections.IEnumerator names = getHeaderNames();
				while (names.MoveNext())
				{
					string name = (string)names.Current;
					System.Collections.IEnumerator values = getHeaders(name);
					while (values.MoveNext())
					{
						string value = (string)values.Current;
						sb.Append("> ").Append(name).Append(": ").Append(value).Append("\n");
					}
				}
				sb.Append(">");
				return sb;
			}
		}

		private class XHttpServletResponse : javax.servlet.http.HttpServletResponseWrapper
		{
			private System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
				<string>> headers = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IList
				<string>>();

			private int status;

			private string message;

			public XHttpServletResponse(javax.servlet.http.HttpServletResponse response)
				: base(response)
			{
			}

			private System.Collections.Generic.IList<string> getHeaderValues(string name, bool
				 reset)
			{
				System.Collections.Generic.IList<string> values = headers[name];
				if (reset || values == null)
				{
					values = new System.Collections.Generic.List<string>();
					headers[name] = values;
				}
				return values;
			}

			public override void addCookie(javax.servlet.http.Cookie cookie)
			{
				base.addCookie(cookie);
				System.Collections.Generic.IList<string> cookies = getHeaderValues("Set-Cookie", 
					false);
				cookies.add(cookie.getName() + "=" + cookie.getValue());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void sendError(int sc, string msg)
			{
				base.sendError(sc, msg);
				status = sc;
				message = msg;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void sendError(int sc)
			{
				base.sendError(sc);
				status = sc;
			}

			public override void setStatus(int sc)
			{
				base.setStatus(sc);
				status = sc;
			}

			public override void setStatus(int sc, string msg)
			{
				base.setStatus(sc, msg);
				status = sc;
				message = msg;
			}

			public override void setHeader(string name, string value)
			{
				base.setHeader(name, value);
				System.Collections.Generic.IList<string> values = getHeaderValues(name, true);
				values.add(value);
			}

			public override void addHeader(string name, string value)
			{
				base.addHeader(name, value);
				System.Collections.Generic.IList<string> values = getHeaderValues(name, false);
				values.add(value);
			}

			public virtual System.Text.StringBuilder getResponseInfo()
			{
				if (status == 0)
				{
					status = 200;
					message = "OK";
				}
				System.Text.StringBuilder sb = new System.Text.StringBuilder(512);
				sb.Append("\n").Append("< ").Append("status code: ").Append(status);
				if (message != null)
				{
					sb.Append(", message: ").Append(message);
				}
				sb.Append("\n");
				foreach (System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.IList
					<string>> entry in headers)
				{
					foreach (string value in entry.Value)
					{
						sb.Append("< ").Append(entry.Key).Append(": ").Append(value).Append("\n");
					}
				}
				sb.Append("<");
				return sb;
			}
		}
	}
}

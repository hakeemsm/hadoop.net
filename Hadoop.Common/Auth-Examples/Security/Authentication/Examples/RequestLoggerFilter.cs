using System.Collections.Generic;
using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Security.Authentication.Examples
{
	/// <summary>Servlet filter that logs HTTP request/response headers</summary>
	public class RequestLoggerFilter : Filter
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(RequestLoggerFilter));

		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void Init(FilterConfig filterConfig)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 filterChain)
		{
			if (!Log.IsDebugEnabled())
			{
				filterChain.DoFilter(request, response);
			}
			else
			{
				RequestLoggerFilter.XHttpServletRequest xRequest = new RequestLoggerFilter.XHttpServletRequest
					((HttpServletRequest)request);
				RequestLoggerFilter.XHttpServletResponse xResponse = new RequestLoggerFilter.XHttpServletResponse
					((HttpServletResponse)response);
				try
				{
					Log.Debug(xRequest.GetResquestInfo().ToString());
					filterChain.DoFilter(xRequest, xResponse);
				}
				finally
				{
					Log.Debug(xResponse.GetResponseInfo().ToString());
				}
			}
		}

		public virtual void Destroy()
		{
		}

		private class XHttpServletRequest : HttpServletRequestWrapper
		{
			public XHttpServletRequest(HttpServletRequest request)
				: base(request)
			{
			}

			public virtual StringBuilder GetResquestInfo()
			{
				StringBuilder sb = new StringBuilder(512);
				sb.Append("\n").Append("> ").Append(GetMethod()).Append(" ").Append(GetRequestURL
					());
				if (GetQueryString() != null)
				{
					sb.Append("?").Append(GetQueryString());
				}
				sb.Append("\n");
				IEnumeration names = GetHeaderNames();
				while (names.MoveNext())
				{
					string name = (string)names.Current;
					IEnumeration values = GetHeaders(name);
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

		private class XHttpServletResponse : HttpServletResponseWrapper
		{
			private IDictionary<string, IList<string>> headers = new Dictionary<string, IList
				<string>>();

			private int status;

			private string message;

			public XHttpServletResponse(HttpServletResponse response)
				: base(response)
			{
			}

			private IList<string> GetHeaderValues(string name, bool reset)
			{
				IList<string> values = headers[name];
				if (reset || values == null)
				{
					values = new AList<string>();
					headers[name] = values;
				}
				return values;
			}

			public override void AddCookie(Cookie cookie)
			{
				base.AddCookie(cookie);
				IList<string> cookies = GetHeaderValues("Set-Cookie", false);
				cookies.AddItem(cookie.GetName() + "=" + cookie.GetValue());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SendError(int sc, string msg)
			{
				base.SendError(sc, msg);
				status = sc;
				message = msg;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SendError(int sc)
			{
				base.SendError(sc);
				status = sc;
			}

			public override void SetStatus(int sc)
			{
				base.SetStatus(sc);
				status = sc;
			}

			public override void SetStatus(int sc, string msg)
			{
				base.SetStatus(sc, msg);
				status = sc;
				message = msg;
			}

			public override void SetHeader(string name, string value)
			{
				base.SetHeader(name, value);
				IList<string> values = GetHeaderValues(name, true);
				values.AddItem(value);
			}

			public override void AddHeader(string name, string value)
			{
				base.AddHeader(name, value);
				IList<string> values = GetHeaderValues(name, false);
				values.AddItem(value);
			}

			public virtual StringBuilder GetResponseInfo()
			{
				if (status == 0)
				{
					status = 200;
					message = "OK";
				}
				StringBuilder sb = new StringBuilder(512);
				sb.Append("\n").Append("< ").Append("status code: ").Append(status);
				if (message != null)
				{
					sb.Append(", message: ").Append(message);
				}
				sb.Append("\n");
				foreach (KeyValuePair<string, IList<string>> entry in headers)
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

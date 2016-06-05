using System.Collections;
using System.Collections.Generic;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>
	/// Subclass of
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationFilter"
	/// 	/>
	/// that
	/// obtains Hadoop-Auth configuration for webhdfs.
	/// </summary>
	public class AuthFilter : AuthenticationFilter
	{
		private const string ConfPrefix = "dfs.web.authentication.";

		/// <summary>
		/// Returns the filter configuration properties,
		/// including the ones prefixed with
		/// <see cref="ConfPrefix"/>
		/// .
		/// The prefix is removed from the returned property names.
		/// </summary>
		/// <param name="prefix">parameter not used.</param>
		/// <param name="config">parameter contains the initialization values.</param>
		/// <returns>Hadoop-Auth configuration properties.</returns>
		/// <exception cref="Javax.Servlet.ServletException"></exception>
		protected override Properties GetConfiguration(string prefix, FilterConfig config
			)
		{
			Properties p = base.GetConfiguration(ConfPrefix, config);
			// set authentication type
			p.SetProperty(AuthType, UserGroupInformation.IsSecurityEnabled() ? KerberosAuthenticationHandler
				.Type : PseudoAuthenticationHandler.Type);
			// if not set, enable anonymous for pseudo authentication
			if (p.GetProperty(PseudoAuthenticationHandler.AnonymousAllowed) == null)
			{
				p.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, "true");
			}
			//set cookie path
			p.SetProperty(CookiePath, "/");
			return p;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 filterChain)
		{
			HttpServletRequest httpRequest = ToLowerCase((HttpServletRequest)request);
			string tokenString = httpRequest.GetParameter(DelegationParam.Name);
			if (tokenString != null)
			{
				//Token is present in the url, therefore token will be used for
				//authentication, bypass kerberos authentication.
				filterChain.DoFilter(httpRequest, response);
				return;
			}
			base.DoFilter(httpRequest, response, filterChain);
		}

		private static HttpServletRequest ToLowerCase(HttpServletRequest request)
		{
			IDictionary<string, string[]> original = (IDictionary<string, string[]>)request.GetParameterMap
				();
			if (!ParamFilter.ContainsUpperCase(original.Keys))
			{
				return request;
			}
			IDictionary<string, IList<string>> m = new Dictionary<string, IList<string>>();
			foreach (KeyValuePair<string, string[]> entry in original)
			{
				string key = StringUtils.ToLowerCase(entry.Key);
				IList<string> strings = m[key];
				if (strings == null)
				{
					strings = new AList<string>();
					m[key] = strings;
				}
				foreach (string v in entry.Value)
				{
					strings.AddItem(v);
				}
			}
			return new _HttpServletRequestWrapper_111(m, request);
		}

		private sealed class _HttpServletRequestWrapper_111 : HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_111(IDictionary<string, IList<string>> m, HttpServletRequest
				 baseArg1)
				: base(baseArg1)
			{
				this.m = m;
				this.parameters = null;
			}

			private IDictionary<string, string[]> parameters;

			public override IDictionary GetParameterMap()
			{
				if (this.parameters == null)
				{
					this.parameters = new Dictionary<string, string[]>();
					foreach (KeyValuePair<string, IList<string>> entry in m)
					{
						IList<string> a = entry.Value;
						this.parameters[entry.Key] = Sharpen.Collections.ToArray(a, new string[a.Count]);
					}
				}
				return this.parameters;
			}

			public override string GetParameter(string name)
			{
				IList<string> a = m[name];
				return a == null ? null : a[0];
			}

			public override string[] GetParameterValues(string name)
			{
				return ((IDictionary<string, string[]>)this.GetParameterMap())[name];
			}

			public override IEnumeration GetParameterNames()
			{
				IEnumerator<string> i = m.Keys.GetEnumerator();
				return new _Enumeration_140(i);
			}

			private sealed class _Enumeration_140 : Enumeration<string>
			{
				public _Enumeration_140(IEnumerator<string> i)
				{
					this.i = i;
				}

				public bool MoveNext()
				{
					return i.HasNext();
				}

				public string Current
				{
					get
					{
						return i.Next();
					}
				}

				private readonly IEnumerator<string> i;
			}

			private readonly IDictionary<string, IList<string>> m;
		}
	}
}

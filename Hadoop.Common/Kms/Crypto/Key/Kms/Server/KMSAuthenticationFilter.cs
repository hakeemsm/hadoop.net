using System.Collections.Generic;
using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// Authentication filter that takes the configuration from the KMS configuration
	/// file.
	/// </summary>
	public class KMSAuthenticationFilter : DelegationTokenAuthenticationFilter
	{
		public const string ConfigPrefix = KMSConfiguration.ConfigPrefix + "authentication.";

		protected override Properties GetConfiguration(string configPrefix, FilterConfig 
			filterConfig)
		{
			Properties props = new Properties();
			Configuration conf = KMSWebApp.GetConfiguration();
			foreach (KeyValuePair<string, string> entry in conf)
			{
				string name = entry.Key;
				if (name.StartsWith(ConfigPrefix))
				{
					string value = conf.Get(name);
					name = Sharpen.Runtime.Substring(name, ConfigPrefix.Length);
					props.SetProperty(name, value);
				}
			}
			string authType = props.GetProperty(AuthType);
			if (authType.Equals(PseudoAuthenticationHandler.Type))
			{
				props.SetProperty(AuthType, typeof(PseudoDelegationTokenAuthenticationHandler).FullName
					);
			}
			else
			{
				if (authType.Equals(KerberosAuthenticationHandler.Type))
				{
					props.SetProperty(AuthType, typeof(KerberosDelegationTokenAuthenticationHandler).
						FullName);
				}
			}
			props.SetProperty(DelegationTokenAuthenticationHandler.TokenKind, KMSClientProvider
				.TokenKind);
			return props;
		}

		protected override Configuration GetProxyuserConfiguration(FilterConfig filterConfig
			)
		{
			IDictionary<string, string> proxyuserConf = KMSWebApp.GetConfiguration().GetValByRegex
				("hadoop\\.kms\\.proxyuser\\.");
			Configuration conf = new Configuration(false);
			foreach (KeyValuePair<string, string> entry in proxyuserConf)
			{
				conf.Set(Sharpen.Runtime.Substring(entry.Key, "hadoop.kms.".Length), entry.Value);
			}
			return conf;
		}

		private class KMSResponse : HttpServletResponseWrapper
		{
			public int statusCode;

			public string msg;

			public KMSResponse(ServletResponse response)
				: base((HttpServletResponse)response)
			{
			}

			public override void SetStatus(int sc)
			{
				statusCode = sc;
				base.SetStatus(sc);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SendError(int sc, string msg)
			{
				statusCode = sc;
				this.msg = msg;
				base.SendError(sc, msg);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SendError(int sc)
			{
				statusCode = sc;
				base.SendError(sc);
			}

			public override void SetStatus(int sc, string sm)
			{
				statusCode = sc;
				msg = sm;
				base.SetStatus(sc, sm);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 filterChain)
		{
			KMSAuthenticationFilter.KMSResponse kmsResponse = new KMSAuthenticationFilter.KMSResponse
				(response);
			base.DoFilter(request, kmsResponse, filterChain);
			if (kmsResponse.statusCode != HttpServletResponse.ScOk && kmsResponse.statusCode 
				!= HttpServletResponse.ScCreated && kmsResponse.statusCode != HttpServletResponse
				.ScUnauthorized)
			{
				KMSWebApp.GetInvalidCallsMeter().Mark();
			}
			// HttpServletResponse.SC_UNAUTHORIZED is because the request does not
			// belong to an authenticated user.
			if (kmsResponse.statusCode == HttpServletResponse.ScUnauthorized)
			{
				KMSWebApp.GetUnauthenticatedCallsMeter().Mark();
				string method = ((HttpServletRequest)request).GetMethod();
				StringBuilder requestURL = ((HttpServletRequest)request).GetRequestURL();
				string queryString = ((HttpServletRequest)request).GetQueryString();
				if (queryString != null)
				{
					requestURL.Append("?").Append(queryString);
				}
				KMSWebApp.GetKMSAudit().Unauthenticated(request.GetRemoteHost(), method, requestURL
					.ToString(), kmsResponse.msg);
			}
		}
	}
}

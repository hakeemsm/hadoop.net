using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security.Http
{
	public class RMAuthenticationFilter : DelegationTokenAuthenticationFilter
	{
		private static AbstractDelegationTokenSecretManager<object> manager;

		private const string OldHeader = "Hadoop-YARN-Auth-Delegation-Token";

		public RMAuthenticationFilter()
		{
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init(FilterConfig filterConfig)
		{
			filterConfig.GetServletContext().SetAttribute(DelegationTokenAuthenticationFilter
				.DelegationTokenSecretManagerAttr, manager);
			base.Init(filterConfig);
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 filterChain)
		{
			HttpServletRequest req = (HttpServletRequest)request;
			string newHeader = req.GetHeader(DelegationTokenAuthenticator.DelegationTokenHeader
				);
			if (newHeader == null || newHeader.IsEmpty())
			{
				// For backward compatibility, allow use of the old header field
				// only when the new header doesn't exist
				string oldHeader = req.GetHeader(OldHeader);
				if (oldHeader != null && !oldHeader.IsEmpty())
				{
					request = new _HttpServletRequestWrapper_70(oldHeader, req);
				}
			}
			base.DoFilter(request, response, filterChain);
		}

		private sealed class _HttpServletRequestWrapper_70 : HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_70(string oldHeader, HttpServletRequest baseArg1
				)
				: base(baseArg1)
			{
				this.oldHeader = oldHeader;
			}

			public override string GetHeader(string name)
			{
				if (name.Equals(DelegationTokenAuthenticator.DelegationTokenHeader))
				{
					return oldHeader;
				}
				return base.GetHeader(name);
			}

			private readonly string oldHeader;
		}

		public static void SetDelegationTokenSecretManager<_T0>(AbstractDelegationTokenSecretManager
			<_T0> manager)
			where _T0 : AbstractDelegationTokenIdentifier
		{
			Org.Apache.Hadoop.Yarn.Server.Security.Http.RMAuthenticationFilter.manager = manager;
		}
	}
}

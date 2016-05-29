using Javax.Servlet.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter
{
	public class AmIpServletRequestWrapper : HttpServletRequestWrapper
	{
		private readonly AmIpPrincipal principal;

		public AmIpServletRequestWrapper(HttpServletRequest request, AmIpPrincipal principal
			)
			: base(request)
		{
			this.principal = principal;
		}

		public override Principal GetUserPrincipal()
		{
			return principal;
		}

		public override string GetRemoteUser()
		{
			return principal.GetName();
		}

		public override bool IsUserInRole(string role)
		{
			//No role info so far
			return false;
		}
	}
}

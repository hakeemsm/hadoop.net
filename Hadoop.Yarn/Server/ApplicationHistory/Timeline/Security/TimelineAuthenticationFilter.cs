using Javax.Servlet;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	public class TimelineAuthenticationFilter : DelegationTokenAuthenticationFilter
	{
		private static TimelineDelegationTokenSecretManagerService.TimelineDelegationTokenSecretManager
			 secretManager;

		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init(FilterConfig filterConfig)
		{
			filterConfig.GetServletContext().SetAttribute(DelegationTokenAuthenticationFilter
				.DelegationTokenSecretManagerAttr, secretManager);
			base.Init(filterConfig);
		}

		public static void SetTimelineDelegationTokenSecretManager(TimelineDelegationTokenSecretManagerService.TimelineDelegationTokenSecretManager
			 secretManager)
		{
			TimelineAuthenticationFilter.secretManager = secretManager;
		}
	}
}

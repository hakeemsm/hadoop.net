using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class CMgrCompletedAppsEvent : ContainerManagerEvent
	{
		private readonly IList<ApplicationId> appsToCleanup;

		private readonly CMgrCompletedAppsEvent.Reason reason;

		public CMgrCompletedAppsEvent(IList<ApplicationId> appsToCleanup, CMgrCompletedAppsEvent.Reason
			 reason)
			: base(ContainerManagerEventType.FinishApps)
		{
			this.appsToCleanup = appsToCleanup;
			this.reason = reason;
		}

		public virtual IList<ApplicationId> GetAppsToCleanup()
		{
			return this.appsToCleanup;
		}

		public virtual CMgrCompletedAppsEvent.Reason GetReason()
		{
			return reason;
		}

		public enum Reason
		{
			OnShutdown,
			ByResourcemanager
		}
	}
}

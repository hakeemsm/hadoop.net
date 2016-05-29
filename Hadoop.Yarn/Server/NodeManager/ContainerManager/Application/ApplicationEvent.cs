using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public class ApplicationEvent : AbstractEvent<ApplicationEventType>
	{
		private readonly ApplicationId applicationID;

		public ApplicationEvent(ApplicationId appID, ApplicationEventType appEventType)
			: base(appEventType)
		{
			this.applicationID = appID;
		}

		public virtual ApplicationId GetApplicationID()
		{
			return this.applicationID;
		}
	}
}

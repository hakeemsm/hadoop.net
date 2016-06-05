using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public class ApplicationInitedEvent : ApplicationEvent
	{
		public ApplicationInitedEvent(ApplicationId appID)
			: base(appID, ApplicationEventType.ApplicationInited)
		{
		}
	}
}

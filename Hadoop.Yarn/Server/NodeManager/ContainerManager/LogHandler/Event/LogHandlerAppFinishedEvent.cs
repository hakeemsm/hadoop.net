using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event
{
	public class LogHandlerAppFinishedEvent : LogHandlerEvent
	{
		private readonly ApplicationId applicationId;

		public LogHandlerAppFinishedEvent(ApplicationId appId)
			: base(LogHandlerEventType.ApplicationFinished)
		{
			this.applicationId = appId;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}
	}
}

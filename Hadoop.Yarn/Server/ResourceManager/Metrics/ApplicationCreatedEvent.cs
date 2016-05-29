using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class ApplicationCreatedEvent : SystemMetricsEvent
	{
		private ApplicationId appId;

		private string name;

		private string type;

		private string user;

		private string queue;

		private long submittedTime;

		public ApplicationCreatedEvent(ApplicationId appId, string name, string type, string
			 user, string queue, long submittedTime, long createdTime)
			: base(SystemMetricsEventType.AppCreated, createdTime)
		{
			this.appId = appId;
			this.name = name;
			this.type = type;
			this.user = user;
			this.queue = queue;
			this.submittedTime = submittedTime;
		}

		public override int GetHashCode()
		{
			return appId.GetHashCode();
		}

		public virtual ApplicationId GetApplicationId()
		{
			return appId;
		}

		public virtual string GetApplicationName()
		{
			return name;
		}

		public virtual string GetApplicationType()
		{
			return type;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual long GetSubmittedTime()
		{
			return submittedTime;
		}
	}
}

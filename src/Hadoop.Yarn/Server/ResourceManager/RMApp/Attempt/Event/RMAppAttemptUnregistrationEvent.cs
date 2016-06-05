using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event
{
	public class RMAppAttemptUnregistrationEvent : RMAppAttemptEvent
	{
		private readonly string finalTrackingUrl;

		private readonly FinalApplicationStatus finalStatus;

		public RMAppAttemptUnregistrationEvent(ApplicationAttemptId appAttemptId, string 
			trackingUrl, FinalApplicationStatus finalStatus, string diagnostics)
			: base(appAttemptId, RMAppAttemptEventType.Unregistered, diagnostics)
		{
			this.finalTrackingUrl = trackingUrl;
			this.finalStatus = finalStatus;
		}

		public virtual string GetFinalTrackingUrl()
		{
			return this.finalTrackingUrl;
		}

		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			return this.finalStatus;
		}
	}
}

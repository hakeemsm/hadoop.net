using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event
{
	public class RMAppAttemptStatusupdateEvent : RMAppAttemptEvent
	{
		private readonly float progress;

		public RMAppAttemptStatusupdateEvent(ApplicationAttemptId appAttemptId, float progress
			)
			: base(appAttemptId, RMAppAttemptEventType.StatusUpdate)
		{
			this.progress = progress;
		}

		public virtual float GetProgress()
		{
			return this.progress;
		}
	}
}

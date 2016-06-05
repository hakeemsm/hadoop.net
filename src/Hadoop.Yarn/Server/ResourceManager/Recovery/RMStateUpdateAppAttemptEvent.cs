using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateUpdateAppAttemptEvent : RMStateStoreEvent
	{
		internal ApplicationAttemptStateData attemptState;

		public RMStateUpdateAppAttemptEvent(ApplicationAttemptStateData attemptState)
			: base(RMStateStoreEventType.UpdateAppAttempt)
		{
			this.attemptState = attemptState;
		}

		public virtual ApplicationAttemptStateData GetAppAttemptState()
		{
			return attemptState;
		}
	}
}

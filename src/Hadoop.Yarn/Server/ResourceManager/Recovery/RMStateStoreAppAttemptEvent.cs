using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreAppAttemptEvent : RMStateStoreEvent
	{
		internal ApplicationAttemptStateData attemptState;

		public RMStateStoreAppAttemptEvent(ApplicationAttemptStateData attemptState)
			: base(RMStateStoreEventType.StoreAppAttempt)
		{
			this.attemptState = attemptState;
		}

		public virtual ApplicationAttemptStateData GetAppAttemptState()
		{
			return attemptState;
		}
	}
}

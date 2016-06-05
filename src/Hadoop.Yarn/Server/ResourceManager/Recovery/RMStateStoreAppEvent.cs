using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreAppEvent : RMStateStoreEvent
	{
		private readonly ApplicationStateData appState;

		public RMStateStoreAppEvent(ApplicationStateData appState)
			: base(RMStateStoreEventType.StoreApp)
		{
			this.appState = appState;
		}

		public virtual ApplicationStateData GetAppState()
		{
			return appState;
		}
	}
}

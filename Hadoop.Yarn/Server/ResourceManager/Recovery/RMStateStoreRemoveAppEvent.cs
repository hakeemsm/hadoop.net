using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreRemoveAppEvent : RMStateStoreEvent
	{
		internal ApplicationStateData appState;

		internal RMStateStoreRemoveAppEvent(ApplicationStateData appState)
			: base(RMStateStoreEventType.RemoveApp)
		{
			this.appState = appState;
		}

		public virtual ApplicationStateData GetAppState()
		{
			return appState;
		}
	}
}

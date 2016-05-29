using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateUpdateAppEvent : RMStateStoreEvent
	{
		private readonly ApplicationStateData appState;

		public RMStateUpdateAppEvent(ApplicationStateData appState)
			: base(RMStateStoreEventType.UpdateApp)
		{
			this.appState = appState;
		}

		public virtual ApplicationStateData GetAppState()
		{
			return appState;
		}
	}
}

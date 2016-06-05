using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppRecoverEvent : RMAppEvent
	{
		private readonly RMStateStore.RMState state;

		public RMAppRecoverEvent(ApplicationId appId, RMStateStore.RMState state)
			: base(appId, RMAppEventType.Recover)
		{
			this.state = state;
		}

		public virtual RMStateStore.RMState GetRMState()
		{
			return state;
		}
	}
}

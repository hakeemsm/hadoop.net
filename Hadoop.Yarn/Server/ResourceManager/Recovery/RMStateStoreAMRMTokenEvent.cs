using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreAMRMTokenEvent : RMStateStoreEvent
	{
		private AMRMTokenSecretManagerState amrmTokenSecretManagerState;

		private bool isUpdate;

		public RMStateStoreAMRMTokenEvent(RMStateStoreEventType type)
			: base(type)
		{
		}

		public RMStateStoreAMRMTokenEvent(AMRMTokenSecretManagerState amrmTokenSecretManagerState
			, bool isUpdate, RMStateStoreEventType type)
			: this(type)
		{
			this.amrmTokenSecretManagerState = amrmTokenSecretManagerState;
			this.isUpdate = isUpdate;
		}

		public virtual AMRMTokenSecretManagerState GetAmrmTokenSecretManagerState()
		{
			return amrmTokenSecretManagerState;
		}

		public virtual bool IsUpdate()
		{
			return isUpdate;
		}
	}
}

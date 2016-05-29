using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreRMDTMasterKeyEvent : RMStateStoreEvent
	{
		private DelegationKey delegationKey;

		public RMStateStoreRMDTMasterKeyEvent(RMStateStoreEventType type)
			: base(type)
		{
		}

		public RMStateStoreRMDTMasterKeyEvent(DelegationKey delegationKey, RMStateStoreEventType
			 type)
			: this(type)
		{
			this.delegationKey = delegationKey;
		}

		public virtual DelegationKey GetDelegationKey()
		{
			return delegationKey;
		}
	}
}

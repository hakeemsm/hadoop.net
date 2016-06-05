using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreRMDTEvent : RMStateStoreEvent
	{
		private RMDelegationTokenIdentifier rmDTIdentifier;

		private long renewDate;

		public RMStateStoreRMDTEvent(RMStateStoreEventType type)
			: base(type)
		{
		}

		public RMStateStoreRMDTEvent(RMDelegationTokenIdentifier rmDTIdentifier, long renewDate
			, RMStateStoreEventType type)
			: this(type)
		{
			this.rmDTIdentifier = rmDTIdentifier;
			this.renewDate = renewDate;
		}

		public virtual RMDelegationTokenIdentifier GetRmDTIdentifier()
		{
			return rmDTIdentifier;
		}

		public virtual long GetRenewDate()
		{
			return renewDate;
		}
	}
}

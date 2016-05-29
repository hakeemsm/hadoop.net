using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public enum RMStateStoreEventType
	{
		StoreAppAttempt,
		StoreApp,
		UpdateApp,
		UpdateAppAttempt,
		RemoveApp,
		Fenced,
		StoreMasterkey,
		RemoveMasterkey,
		StoreDelegationToken,
		RemoveDelegationToken,
		UpdateDelegationToken,
		UpdateAmrmToken
	}
}

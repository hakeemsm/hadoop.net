using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public enum RMAppEventType
	{
		Start,
		Recover,
		Kill,
		Move,
		AppRejected,
		AppAccepted,
		AttemptRegistered,
		AttemptUnregistered,
		AttemptFinished,
		AttemptFailed,
		AttemptKilled,
		NodeUpdate,
		AppRunningOnNode,
		AppNewSaved,
		AppUpdateSaved
	}
}

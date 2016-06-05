using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public enum RMAppAttemptEventType
	{
		Start,
		Kill,
		Launched,
		LaunchFailed,
		Expire,
		Registered,
		StatusUpdate,
		Unregistered,
		ContainerAllocated,
		ContainerFinished,
		AttemptNewSaved,
		AttemptUpdateSaved,
		AttemptAdded,
		Recover
	}
}

using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public enum RMAppAttemptState
	{
		New,
		Submitted,
		Scheduled,
		Allocated,
		Launched,
		Failed,
		Running,
		Finishing,
		Finished,
		Killed,
		AllocatedSaving,
		LaunchedUnmanagedSaving,
		FinalSaving
	}
}

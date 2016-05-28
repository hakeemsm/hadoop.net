using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job
{
	/// <summary>TaskAttemptImpl internal state machine states.</summary>
	public enum TaskAttemptStateInternal
	{
		New,
		Unassigned,
		Assigned,
		Running,
		CommitPending,
		SuccessContainerCleanup,
		Succeeded,
		FailContainerCleanup,
		FailTaskCleanup,
		Failed,
		KillContainerCleanup,
		KillTaskCleanup,
		Killed
	}
}

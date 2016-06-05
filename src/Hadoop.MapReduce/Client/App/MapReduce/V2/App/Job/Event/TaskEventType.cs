using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	/// <summary>Event types handled by Task.</summary>
	public enum TaskEventType
	{
		TKill,
		TSchedule,
		TRecover,
		TAddSpecAttempt,
		TAttemptLaunched,
		TAttemptCommitPending,
		TAttemptFailed,
		TAttemptSucceeded,
		TAttemptKilled
	}
}

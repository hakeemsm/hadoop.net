using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	/// <summary>Event types handled by TaskAttempt.</summary>
	public enum TaskAttemptEventType
	{
		TaSchedule,
		TaReschedule,
		TaRecover,
		TaKill,
		TaAssigned,
		TaContainerCompleted,
		TaContainerLaunched,
		TaContainerLaunchFailed,
		TaContainerCleaned,
		TaDiagnosticsUpdate,
		TaCommitPending,
		TaDone,
		TaFailmsg,
		TaUpdate,
		TaTimedOut,
		TaCleanupDone,
		TaTooManyFetchFailure
	}
}

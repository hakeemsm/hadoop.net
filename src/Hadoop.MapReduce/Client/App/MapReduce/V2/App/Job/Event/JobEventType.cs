using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	/// <summary>Event types handled by Job.</summary>
	public enum JobEventType
	{
		JobKill,
		JobInit,
		JobInitFailed,
		JobStart,
		JobTaskCompleted,
		JobMapTaskRescheduled,
		JobTaskAttemptCompleted,
		JobSetupCompleted,
		JobSetupFailed,
		JobCommitCompleted,
		JobCommitFailed,
		JobAbortCompleted,
		JobCompleted,
		JobFailWaitTimedout,
		JobDiagnosticUpdate,
		InternalError,
		JobCounterUpdate,
		JobTaskAttemptFetchFailure,
		JobUpdatedNodes,
		JobAmReboot
	}
}

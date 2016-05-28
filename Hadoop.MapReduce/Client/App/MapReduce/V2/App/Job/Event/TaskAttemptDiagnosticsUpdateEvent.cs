using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskAttemptDiagnosticsUpdateEvent : TaskAttemptEvent
	{
		private string diagnosticInfo;

		public TaskAttemptDiagnosticsUpdateEvent(TaskAttemptId attemptID, string diagnosticInfo
			)
			: base(attemptID, TaskAttemptEventType.TaDiagnosticsUpdate)
		{
			this.diagnosticInfo = diagnosticInfo;
		}

		public virtual string GetDiagnosticInfo()
		{
			return diagnosticInfo;
		}
	}
}

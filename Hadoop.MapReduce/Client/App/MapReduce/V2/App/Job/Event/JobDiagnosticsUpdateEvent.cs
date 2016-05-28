using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobDiagnosticsUpdateEvent : JobEvent
	{
		private string diagnosticUpdate;

		public JobDiagnosticsUpdateEvent(JobId jobID, string diagnostic)
			: base(jobID, JobEventType.JobDiagnosticUpdate)
		{
			this.diagnosticUpdate = diagnostic;
		}

		public virtual string GetDiagnosticUpdate()
		{
			return this.diagnosticUpdate;
		}
	}
}

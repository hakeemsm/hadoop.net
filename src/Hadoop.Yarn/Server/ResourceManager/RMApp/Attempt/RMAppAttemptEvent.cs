using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class RMAppAttemptEvent : AbstractEvent<RMAppAttemptEventType>
	{
		private readonly ApplicationAttemptId appAttemptId;

		private readonly string diagnosticMsg;

		public RMAppAttemptEvent(ApplicationAttemptId appAttemptId, RMAppAttemptEventType
			 type)
			: this(appAttemptId, type, string.Empty)
		{
		}

		public RMAppAttemptEvent(ApplicationAttemptId appAttemptId, RMAppAttemptEventType
			 type, string diagnostics)
			: base(type)
		{
			this.appAttemptId = appAttemptId;
			this.diagnosticMsg = diagnostics;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return this.appAttemptId;
		}

		public virtual string GetDiagnosticMsg()
		{
			return diagnosticMsg;
		}
	}
}

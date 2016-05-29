using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppEvent : AbstractEvent<RMAppEventType>
	{
		private readonly ApplicationId appId;

		private readonly string diagnosticMsg;

		public RMAppEvent(ApplicationId appId, RMAppEventType type)
			: this(appId, type, string.Empty)
		{
		}

		public RMAppEvent(ApplicationId appId, RMAppEventType type, string diagnostic)
			: base(type)
		{
			this.appId = appId;
			this.diagnosticMsg = diagnostic;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return this.appId;
		}

		public virtual string GetDiagnosticMsg()
		{
			return this.diagnosticMsg;
		}
	}
}

using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	/// <summary>Finish/abort event</summary>
	public class ApplicationFinishEvent : ApplicationEvent
	{
		private readonly string diagnostic;

		/// <summary>Application event to abort all containers associated with the app</summary>
		/// <param name="appId">to abort containers</param>
		/// <param name="diagnostic">reason for the abort</param>
		public ApplicationFinishEvent(ApplicationId appId, string diagnostic)
			: base(appId, ApplicationEventType.FinishApplication)
		{
			this.diagnostic = diagnostic;
		}

		/// <summary>Why the app was aborted</summary>
		/// <returns>diagnostic message</returns>
		public virtual string GetDiagnostic()
		{
			return diagnostic;
		}
	}
}

using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppFailedAttemptEvent : RMAppEvent
	{
		private readonly bool transferStateFromPreviousAttempt;

		public RMAppFailedAttemptEvent(ApplicationId appId, RMAppEventType @event, string
			 diagnostics, bool transferStateFromPreviousAttempt)
			: base(appId, @event, diagnostics)
		{
			this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
		}

		public virtual bool GetTransferStateFromPreviousAttempt()
		{
			return transferStateFromPreviousAttempt;
		}
	}
}

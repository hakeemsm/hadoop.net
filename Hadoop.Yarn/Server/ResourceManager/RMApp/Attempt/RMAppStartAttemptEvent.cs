using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class RMAppStartAttemptEvent : RMAppAttemptEvent
	{
		private readonly bool transferStateFromPreviousAttempt;

		public RMAppStartAttemptEvent(ApplicationAttemptId appAttemptId, bool transferStateFromPreviousAttempt
			)
			: base(appAttemptId, RMAppAttemptEventType.Start)
		{
			this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
		}

		public virtual bool GetTransferStateFromPreviousAttempt()
		{
			return transferStateFromPreviousAttempt;
		}
	}
}

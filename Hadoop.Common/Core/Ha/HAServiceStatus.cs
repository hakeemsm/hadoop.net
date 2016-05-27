using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class HAServiceStatus
	{
		private HAServiceProtocol.HAServiceState state;

		private bool readyToBecomeActive;

		private string notReadyReason;

		public HAServiceStatus(HAServiceProtocol.HAServiceState state)
		{
			this.state = state;
		}

		public virtual HAServiceProtocol.HAServiceState GetState()
		{
			return state;
		}

		public virtual Org.Apache.Hadoop.HA.HAServiceStatus SetReadyToBecomeActive()
		{
			this.readyToBecomeActive = true;
			this.notReadyReason = null;
			return this;
		}

		public virtual Org.Apache.Hadoop.HA.HAServiceStatus SetNotReadyToBecomeActive(string
			 reason)
		{
			this.readyToBecomeActive = false;
			this.notReadyReason = reason;
			return this;
		}

		public virtual bool IsReadyToBecomeActive()
		{
			return readyToBecomeActive;
		}

		public virtual string GetNotReadyReason()
		{
			return notReadyReason;
		}
	}
}

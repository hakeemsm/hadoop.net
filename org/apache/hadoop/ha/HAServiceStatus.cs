using Sharpen;

namespace org.apache.hadoop.ha
{
	public class HAServiceStatus
	{
		private org.apache.hadoop.ha.HAServiceProtocol.HAServiceState state;

		private bool readyToBecomeActive;

		private string notReadyReason;

		public HAServiceStatus(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState state
			)
		{
			this.state = state;
		}

		public virtual org.apache.hadoop.ha.HAServiceProtocol.HAServiceState getState()
		{
			return state;
		}

		public virtual org.apache.hadoop.ha.HAServiceStatus setReadyToBecomeActive()
		{
			this.readyToBecomeActive = true;
			this.notReadyReason = null;
			return this;
		}

		public virtual org.apache.hadoop.ha.HAServiceStatus setNotReadyToBecomeActive(string
			 reason)
		{
			this.readyToBecomeActive = false;
			this.notReadyReason = reason;
			return this;
		}

		public virtual bool isReadyToBecomeActive()
		{
			return readyToBecomeActive;
		}

		public virtual string getNotReadyReason()
		{
			return notReadyReason;
		}
	}
}

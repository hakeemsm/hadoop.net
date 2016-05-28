using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Response to a journal fence request.</summary>
	/// <remarks>
	/// Response to a journal fence request. See
	/// <see cref="JournalProtocol.Fence(JournalInfo, long, string)"/>
	/// </remarks>
	public class FenceResponse
	{
		private readonly long previousEpoch;

		private readonly long lastTransactionId;

		private readonly bool isInSync;

		public FenceResponse(long previousEpoch, long lastTransId, bool inSync)
		{
			this.previousEpoch = previousEpoch;
			this.lastTransactionId = lastTransId;
			this.isInSync = inSync;
		}

		public virtual bool IsInSync()
		{
			return isInSync;
		}

		public virtual long GetLastTransactionId()
		{
			return lastTransactionId;
		}

		public virtual long GetPreviousEpoch()
		{
			return previousEpoch;
		}
	}
}

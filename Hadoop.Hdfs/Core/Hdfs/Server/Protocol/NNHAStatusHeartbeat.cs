using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	public class NNHAStatusHeartbeat
	{
		private readonly HAServiceProtocol.HAServiceState state;

		private long txid = HdfsConstants.InvalidTxid;

		public NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState state, long txid)
		{
			this.state = state;
			this.txid = txid;
		}

		public virtual HAServiceProtocol.HAServiceState GetState()
		{
			return state;
		}

		public virtual long GetTxId()
		{
			return txid;
		}
	}
}

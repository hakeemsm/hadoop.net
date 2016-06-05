using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Replica recovery information.</summary>
	public class ReplicaRecoveryInfo : Block
	{
		private readonly HdfsServerConstants.ReplicaState originalState;

		public ReplicaRecoveryInfo(long blockId, long diskLen, long gs, HdfsServerConstants.ReplicaState
			 rState)
		{
			Set(blockId, diskLen, gs);
			originalState = rState;
		}

		public virtual HdfsServerConstants.ReplicaState GetOriginalReplicaState()
		{
			return originalState;
		}

		public override bool Equals(object o)
		{
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}
	}
}

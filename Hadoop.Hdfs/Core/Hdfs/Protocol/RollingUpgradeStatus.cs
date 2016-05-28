using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Rolling upgrade status</summary>
	public class RollingUpgradeStatus
	{
		private readonly string blockPoolId;

		private readonly bool finalized;

		public RollingUpgradeStatus(string blockPoolId, bool finalized)
		{
			this.blockPoolId = blockPoolId;
			this.finalized = finalized;
		}

		public virtual string GetBlockPoolId()
		{
			return blockPoolId;
		}

		public virtual bool IsFinalized()
		{
			return finalized;
		}

		public override int GetHashCode()
		{
			return blockPoolId.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeStatus))
				{
					return false;
				}
			}
			Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeStatus that = (Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeStatus
				)obj;
			return this.blockPoolId.Equals(that.blockPoolId) && this.IsFinalized() == that.IsFinalized
				();
		}

		public override string ToString()
		{
			return "  Block Pool ID: " + blockPoolId;
		}
	}
}

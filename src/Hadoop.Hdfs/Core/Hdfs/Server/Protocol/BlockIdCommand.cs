using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// A BlockIdCommand is an instruction to a datanode
	/// regarding some blocks under its control.
	/// </summary>
	public class BlockIdCommand : DatanodeCommand
	{
		internal readonly string poolId;

		internal readonly long[] blockIds;

		/// <summary>Create BlockCommand for the given action</summary>
		public BlockIdCommand(int action, string poolId, long[] blockIds)
			: base(action)
		{
			this.poolId = poolId;
			this.blockIds = blockIds;
		}

		public virtual string GetBlockPoolId()
		{
			return poolId;
		}

		public virtual long[] GetBlockIds()
		{
			return blockIds;
		}
	}
}

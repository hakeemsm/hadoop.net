using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// A BlockCommand is an instruction to a datanode
	/// regarding some blocks under its control.
	/// </summary>
	/// <remarks>
	/// A BlockCommand is an instruction to a datanode
	/// regarding some blocks under its control.  It tells
	/// the DataNode to either invalidate a set of indicated
	/// blocks, or to copy a set of indicated blocks to
	/// another DataNode.
	/// </remarks>
	public class BlockCommand : DatanodeCommand
	{
		/// <summary>
		/// This constant is used to indicate that the block deletion does not need
		/// explicit ACK from the datanode.
		/// </summary>
		/// <remarks>
		/// This constant is used to indicate that the block deletion does not need
		/// explicit ACK from the datanode. When a block is put into the list of blocks
		/// to be deleted, it's size is set to this constant. We assume that no block
		/// would actually have this size. Otherwise, we would miss ACKs for blocks
		/// with such size. Positive number is used for compatibility reasons.
		/// </remarks>
		public const long NoAck = long.MaxValue;

		internal readonly string poolId;

		internal readonly Block[] blocks;

		internal readonly DatanodeInfo[][] targets;

		internal readonly StorageType[][] targetStorageTypes;

		internal readonly string[][] targetStorageIDs;

		/// <summary>Create BlockCommand for transferring blocks to another datanode</summary>
		/// <param name="blocktargetlist">blocks to be transferred</param>
		public BlockCommand(int action, string poolId, IList<DatanodeDescriptor.BlockTargetPair
			> blocktargetlist)
			: base(action)
		{
			this.poolId = poolId;
			blocks = new Block[blocktargetlist.Count];
			targets = new DatanodeInfo[blocks.Length][];
			targetStorageTypes = new StorageType[blocks.Length][];
			targetStorageIDs = new string[blocks.Length][];
			for (int i = 0; i < blocks.Length; i++)
			{
				DatanodeDescriptor.BlockTargetPair p = blocktargetlist[i];
				blocks[i] = p.block;
				targets[i] = DatanodeStorageInfo.ToDatanodeInfos(p.targets);
				targetStorageTypes[i] = DatanodeStorageInfo.ToStorageTypes(p.targets);
				targetStorageIDs[i] = DatanodeStorageInfo.ToStorageIDs(p.targets);
			}
		}

		private static readonly DatanodeInfo[][] EmptyTargetDatanodes = new DatanodeInfo[
			][] {  };

		private static readonly StorageType[][] EmptyTargetStorageTypes = new StorageType
			[][] {  };

		private static readonly string[][] EmptyTargetStorageids = new string[][] {  };

		/// <summary>Create BlockCommand for the given action</summary>
		/// <param name="blocks">blocks related to the action</param>
		public BlockCommand(int action, string poolId, Block[] blocks)
			: this(action, poolId, blocks, EmptyTargetDatanodes, EmptyTargetStorageTypes, EmptyTargetStorageids
				)
		{
		}

		/// <summary>Create BlockCommand for the given action</summary>
		/// <param name="blocks">blocks related to the action</param>
		public BlockCommand(int action, string poolId, Block[] blocks, DatanodeInfo[][] targets
			, StorageType[][] targetStorageTypes, string[][] targetStorageIDs)
			: base(action)
		{
			this.poolId = poolId;
			this.blocks = blocks;
			this.targets = targets;
			this.targetStorageTypes = targetStorageTypes;
			this.targetStorageIDs = targetStorageIDs;
		}

		public virtual string GetBlockPoolId()
		{
			return poolId;
		}

		public virtual Block[] GetBlocks()
		{
			return blocks;
		}

		public virtual DatanodeInfo[][] GetTargets()
		{
			return targets;
		}

		public virtual StorageType[][] GetTargetStorageTypes()
		{
			return targetStorageTypes;
		}

		public virtual string[][] GetTargetStorageIDs()
		{
			return targetStorageIDs;
		}
	}
}

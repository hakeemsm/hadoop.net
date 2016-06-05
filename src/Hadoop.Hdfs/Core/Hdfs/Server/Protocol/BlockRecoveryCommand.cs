using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// BlockRecoveryCommand is an instruction to a data-node to recover
	/// the specified blocks.
	/// </summary>
	/// <remarks>
	/// BlockRecoveryCommand is an instruction to a data-node to recover
	/// the specified blocks.
	/// The data-node that receives this command treats itself as a primary
	/// data-node in the recover process.
	/// Block recovery is identified by a recoveryId, which is also the new
	/// generation stamp, which the block will have after the recovery succeeds.
	/// </remarks>
	public class BlockRecoveryCommand : DatanodeCommand
	{
		internal readonly ICollection<BlockRecoveryCommand.RecoveringBlock> recoveringBlocks;

		/// <summary>
		/// This is a block with locations from which it should be recovered
		/// and the new generation stamp, which the block will have after
		/// successful recovery.
		/// </summary>
		/// <remarks>
		/// This is a block with locations from which it should be recovered
		/// and the new generation stamp, which the block will have after
		/// successful recovery.
		/// The new generation stamp of the block, also plays role of the recovery id.
		/// </remarks>
		public class RecoveringBlock : LocatedBlock
		{
			private readonly long newGenerationStamp;

			private readonly Block recoveryBlock;

			/// <summary>Create RecoveringBlock.</summary>
			public RecoveringBlock(ExtendedBlock b, DatanodeInfo[] locs, long newGS)
				: base(b, locs, -1, false)
			{
				// startOffset is unknown
				this.newGenerationStamp = newGS;
				this.recoveryBlock = null;
			}

			/// <summary>Create RecoveringBlock with copy-on-truncate option.</summary>
			public RecoveringBlock(ExtendedBlock b, DatanodeInfo[] locs, Block recoveryBlock)
				: base(b, locs, -1, false)
			{
				// startOffset is unknown
				this.newGenerationStamp = recoveryBlock.GetGenerationStamp();
				this.recoveryBlock = recoveryBlock;
			}

			/// <summary>
			/// Return the new generation stamp of the block,
			/// which also plays role of the recovery id.
			/// </summary>
			public virtual long GetNewGenerationStamp()
			{
				return newGenerationStamp;
			}

			/// <summary>Return the new block.</summary>
			public virtual Block GetNewBlock()
			{
				return recoveryBlock;
			}
		}

		/// <summary>Create empty BlockRecoveryCommand.</summary>
		public BlockRecoveryCommand()
			: this(0)
		{
		}

		/// <summary>
		/// Create BlockRecoveryCommand with
		/// the specified capacity for recovering blocks.
		/// </summary>
		public BlockRecoveryCommand(int capacity)
			: this(new AList<BlockRecoveryCommand.RecoveringBlock>(capacity))
		{
		}

		public BlockRecoveryCommand(ICollection<BlockRecoveryCommand.RecoveringBlock> blocks
			)
			: base(DatanodeProtocol.DnaRecoverblock)
		{
			recoveringBlocks = blocks;
		}

		/// <summary>Return the list of recovering blocks.</summary>
		public virtual ICollection<BlockRecoveryCommand.RecoveringBlock> GetRecoveringBlocks
			()
		{
			return recoveringBlocks;
		}

		/// <summary>Add recovering block to the command.</summary>
		public virtual void Add(BlockRecoveryCommand.RecoveringBlock block)
		{
			recoveringBlocks.AddItem(block);
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("BlockRecoveryCommand(\n  ");
			Joiner.On("\n  ").AppendTo(sb, recoveringBlocks);
			sb.Append("\n)");
			return sb.ToString();
		}
	}
}

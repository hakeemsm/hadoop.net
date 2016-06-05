using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>A data structure to store the blocks in an incremental block report.</summary>
	public class ReceivedDeletedBlockInfo
	{
		internal Block block;

		internal ReceivedDeletedBlockInfo.BlockStatus status;

		internal string delHints;

		[System.Serializable]
		public sealed class BlockStatus
		{
			public static readonly ReceivedDeletedBlockInfo.BlockStatus ReceivingBlock = new 
				ReceivedDeletedBlockInfo.BlockStatus(1);

			public static readonly ReceivedDeletedBlockInfo.BlockStatus ReceivedBlock = new ReceivedDeletedBlockInfo.BlockStatus
				(2);

			public static readonly ReceivedDeletedBlockInfo.BlockStatus DeletedBlock = new ReceivedDeletedBlockInfo.BlockStatus
				(3);

			private readonly int code;

			internal BlockStatus(int code)
			{
				this.code = code;
			}

			public int GetCode()
			{
				return ReceivedDeletedBlockInfo.BlockStatus.code;
			}

			public static ReceivedDeletedBlockInfo.BlockStatus FromCode(int code)
			{
				foreach (ReceivedDeletedBlockInfo.BlockStatus bs in ReceivedDeletedBlockInfo.BlockStatus
					.Values())
				{
					if (bs.code == code)
					{
						return bs;
					}
				}
				return null;
			}
		}

		public ReceivedDeletedBlockInfo()
		{
		}

		public ReceivedDeletedBlockInfo(Block blk, ReceivedDeletedBlockInfo.BlockStatus status
			, string delHints)
		{
			this.block = blk;
			this.status = status;
			this.delHints = delHints;
		}

		public virtual Block GetBlock()
		{
			return this.block;
		}

		public virtual void SetBlock(Block blk)
		{
			this.block = blk;
		}

		public virtual string GetDelHints()
		{
			return this.delHints;
		}

		public virtual void SetDelHints(string hints)
		{
			this.delHints = hints;
		}

		public virtual ReceivedDeletedBlockInfo.BlockStatus GetStatus()
		{
			return status;
		}

		public override bool Equals(object o)
		{
			if (!(o is ReceivedDeletedBlockInfo))
			{
				return false;
			}
			ReceivedDeletedBlockInfo other = (ReceivedDeletedBlockInfo)o;
			return this.block.Equals(other.GetBlock()) && this.status == other.status && this
				.delHints != null && this.delHints.Equals(other.delHints);
		}

		public override int GetHashCode()
		{
			System.Diagnostics.Debug.Assert(false, "hashCode not designed");
			return 0;
		}

		public virtual bool BlockEquals(Block b)
		{
			return this.block.Equals(b);
		}

		public virtual bool IsDeletedBlock()
		{
			return status == ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock;
		}

		public override string ToString()
		{
			return block.ToString() + ", status: " + status + ", delHint: " + delHints;
		}
	}
}

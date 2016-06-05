using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Identifies a Block uniquely across the block pools</summary>
	public class ExtendedBlock
	{
		private string poolId;

		private Block block;

		public ExtendedBlock()
			: this(null, 0, 0, 0)
		{
		}

		public ExtendedBlock(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock b)
			: this(b.poolId, new Block(b.block))
		{
		}

		public ExtendedBlock(string poolId, long blockId)
			: this(poolId, blockId, 0, 0)
		{
		}

		public ExtendedBlock(string poolId, Block b)
		{
			this.poolId = poolId;
			this.block = b;
		}

		public ExtendedBlock(string poolId, long blkid, long len, long genstamp)
		{
			this.poolId = poolId;
			block = new Block(blkid, len, genstamp);
		}

		public virtual string GetBlockPoolId()
		{
			return poolId;
		}

		/// <summary>Returns the block file name for the block</summary>
		public virtual string GetBlockName()
		{
			return block.GetBlockName();
		}

		public virtual long GetNumBytes()
		{
			return block.GetNumBytes();
		}

		public virtual long GetBlockId()
		{
			return block.GetBlockId();
		}

		public virtual long GetGenerationStamp()
		{
			return block.GetGenerationStamp();
		}

		public virtual void SetBlockId(long bid)
		{
			block.SetBlockId(bid);
		}

		public virtual void SetGenerationStamp(long genStamp)
		{
			block.SetGenerationStamp(genStamp);
		}

		public virtual void SetNumBytes(long len)
		{
			block.SetNumBytes(len);
		}

		public virtual void Set(string poolId, Block blk)
		{
			this.poolId = poolId;
			this.block = blk;
		}

		public static Block GetLocalBlock(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock b
			)
		{
			return b == null ? null : b.GetLocalBlock();
		}

		public virtual Block GetLocalBlock()
		{
			return block;
		}

		public override bool Equals(object o)
		{
			// Object
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock b = (Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock
				)o;
			return b.block.Equals(block) && b.poolId.Equals(poolId);
		}

		public override int GetHashCode()
		{
			// Object
			int result = 31 + poolId.GetHashCode();
			return (31 * result + block.GetHashCode());
		}

		public override string ToString()
		{
			// Object
			return poolId + ":" + block;
		}
	}
}

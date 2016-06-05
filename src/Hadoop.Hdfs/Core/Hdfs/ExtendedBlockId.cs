using System.Text;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>An immutable key which identifies a block.</summary>
	public sealed class ExtendedBlockId
	{
		/// <summary>The block ID for this block.</summary>
		private readonly long blockId;

		/// <summary>The block pool ID for this block.</summary>
		private readonly string bpId;

		public static Org.Apache.Hadoop.Hdfs.ExtendedBlockId FromExtendedBlock(ExtendedBlock
			 block)
		{
			return new Org.Apache.Hadoop.Hdfs.ExtendedBlockId(block.GetBlockId(), block.GetBlockPoolId
				());
		}

		public ExtendedBlockId(long blockId, string bpId)
		{
			this.blockId = blockId;
			this.bpId = bpId;
		}

		public long GetBlockId()
		{
			return this.blockId;
		}

		public string GetBlockPoolId()
		{
			return this.bpId;
		}

		public override bool Equals(object o)
		{
			if ((o == null) || (o.GetType() != this.GetType()))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.ExtendedBlockId other = (Org.Apache.Hadoop.Hdfs.ExtendedBlockId
				)o;
			return new EqualsBuilder().Append(blockId, other.blockId).Append(bpId, other.bpId
				).IsEquals();
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder().Append(this.blockId).Append(this.bpId).ToHashCode();
		}

		public override string ToString()
		{
			return new StringBuilder().Append(blockId).Append("_").Append(bpId).ToString();
		}
	}
}

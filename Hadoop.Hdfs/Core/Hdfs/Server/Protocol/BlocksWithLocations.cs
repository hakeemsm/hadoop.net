using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Maintains an array of blocks and their corresponding storage IDs.</summary>
	public class BlocksWithLocations
	{
		/// <summary>A class to keep track of a block and its locations</summary>
		public class BlockWithLocations
		{
			internal readonly Block block;

			internal readonly string[] datanodeUuids;

			internal readonly string[] storageIDs;

			internal readonly StorageType[] storageTypes;

			/// <summary>constructor</summary>
			public BlockWithLocations(Block block, string[] datanodeUuids, string[] storageIDs
				, StorageType[] storageTypes)
			{
				this.block = block;
				this.datanodeUuids = datanodeUuids;
				this.storageIDs = storageIDs;
				this.storageTypes = storageTypes;
			}

			/// <summary>get the block</summary>
			public virtual Block GetBlock()
			{
				return block;
			}

			/// <summary>get the block's datanode locations</summary>
			public virtual string[] GetDatanodeUuids()
			{
				return datanodeUuids;
			}

			/// <summary>get the block's storage locations</summary>
			public virtual string[] GetStorageIDs()
			{
				return storageIDs;
			}

			/// <returns>the storage types</returns>
			public virtual StorageType[] GetStorageTypes()
			{
				return storageTypes;
			}

			public override string ToString()
			{
				StringBuilder b = new StringBuilder();
				b.Append(block);
				if (datanodeUuids.Length == 0)
				{
					return b.Append("[]").ToString();
				}
				AppendString(0, b.Append("["));
				for (int i = 1; i < datanodeUuids.Length; i++)
				{
					AppendString(i, b.Append(","));
				}
				return b.Append("]").ToString();
			}

			private StringBuilder AppendString(int i, StringBuilder b)
			{
				return b.Append("[").Append(storageTypes[i]).Append("]").Append(storageIDs[i]).Append
					("@").Append(datanodeUuids[i]);
			}
		}

		private readonly BlocksWithLocations.BlockWithLocations[] blocks;

		/// <summary>Constructor with one parameter</summary>
		public BlocksWithLocations(BlocksWithLocations.BlockWithLocations[] blocks)
		{
			this.blocks = blocks;
		}

		/// <summary>getter</summary>
		public virtual BlocksWithLocations.BlockWithLocations[] GetBlocks()
		{
			return blocks;
		}
	}
}

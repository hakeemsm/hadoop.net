using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// Report of block received and deleted per Datanode
	/// storage.
	/// </summary>
	public class StorageReceivedDeletedBlocks
	{
		internal readonly DatanodeStorage storage;

		private readonly ReceivedDeletedBlockInfo[] blocks;

		[Obsolete]
		public virtual string GetStorageID()
		{
			return storage.GetStorageID();
		}

		public virtual DatanodeStorage GetStorage()
		{
			return storage;
		}

		public virtual ReceivedDeletedBlockInfo[] GetBlocks()
		{
			return blocks;
		}

		[Obsolete]
		public StorageReceivedDeletedBlocks(string storageID, ReceivedDeletedBlockInfo[] 
			blocks)
		{
			this.storage = new DatanodeStorage(storageID);
			this.blocks = blocks;
		}

		public StorageReceivedDeletedBlocks(DatanodeStorage storage, ReceivedDeletedBlockInfo
			[] blocks)
		{
			this.storage = storage;
			this.blocks = blocks;
		}
	}
}

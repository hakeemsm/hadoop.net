using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Utilization report for a Datanode storage</summary>
	public class StorageReport
	{
		private readonly DatanodeStorage storage;

		private readonly bool failed;

		private readonly long capacity;

		private readonly long dfsUsed;

		private readonly long remaining;

		private readonly long blockPoolUsed;

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Protocol.StorageReport[] EmptyArray
			 = new Org.Apache.Hadoop.Hdfs.Server.Protocol.StorageReport[] {  };

		public StorageReport(DatanodeStorage storage, bool failed, long capacity, long dfsUsed
			, long remaining, long bpUsed)
		{
			this.storage = storage;
			this.failed = failed;
			this.capacity = capacity;
			this.dfsUsed = dfsUsed;
			this.remaining = remaining;
			this.blockPoolUsed = bpUsed;
		}

		public virtual DatanodeStorage GetStorage()
		{
			return storage;
		}

		public virtual bool IsFailed()
		{
			return failed;
		}

		public virtual long GetCapacity()
		{
			return capacity;
		}

		public virtual long GetDfsUsed()
		{
			return dfsUsed;
		}

		public virtual long GetRemaining()
		{
			return remaining;
		}

		public virtual long GetBlockPoolUsed()
		{
			return blockPoolUsed;
		}
	}
}

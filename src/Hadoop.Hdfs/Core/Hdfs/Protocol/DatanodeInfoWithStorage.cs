using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	public class DatanodeInfoWithStorage : DatanodeInfo
	{
		private readonly string storageID;

		private readonly StorageType storageType;

		public DatanodeInfoWithStorage(DatanodeInfo from, string storageID, StorageType storageType
			)
			: base(from)
		{
			this.storageID = storageID;
			this.storageType = storageType;
			SetSoftwareVersion(from.GetSoftwareVersion());
			SetDependentHostNames(from.GetDependentHostNames());
			SetLevel(from.GetLevel());
			SetParent(from.GetParent());
		}

		public virtual string GetStorageID()
		{
			return storageID;
		}

		public virtual StorageType GetStorageType()
		{
			return storageType;
		}

		public override bool Equals(object o)
		{
			// allows this class to be used interchangeably with DatanodeInfo
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			// allows this class to be used interchangeably with DatanodeInfo
			return base.GetHashCode();
		}

		public override string ToString()
		{
			return "DatanodeInfoWithStorage[" + base.ToString() + "," + storageID + "," + storageType
				 + "]";
		}
	}
}

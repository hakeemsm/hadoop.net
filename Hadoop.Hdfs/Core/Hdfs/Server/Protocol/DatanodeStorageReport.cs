using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Class captures information of a datanode and its storages.</summary>
	public class DatanodeStorageReport
	{
		internal readonly DatanodeInfo datanodeInfo;

		internal readonly StorageReport[] storageReports;

		public DatanodeStorageReport(DatanodeInfo datanodeInfo, StorageReport[] storageReports
			)
		{
			this.datanodeInfo = datanodeInfo;
			this.storageReports = storageReports;
		}

		public virtual DatanodeInfo GetDatanodeInfo()
		{
			return datanodeInfo;
		}

		public virtual StorageReport[] GetStorageReports()
		{
			return storageReports;
		}
	}
}

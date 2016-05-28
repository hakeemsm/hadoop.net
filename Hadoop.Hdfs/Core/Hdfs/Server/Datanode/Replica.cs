using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This represents block replicas which are stored in DataNode.</summary>
	public interface Replica
	{
		/// <summary>Get the block ID</summary>
		long GetBlockId();

		/// <summary>Get the generation stamp</summary>
		long GetGenerationStamp();

		/// <summary>Get the replica state</summary>
		/// <returns>the replica state</returns>
		HdfsServerConstants.ReplicaState GetState();

		/// <summary>Get the number of bytes received</summary>
		/// <returns>the number of bytes that have been received</returns>
		long GetNumBytes();

		/// <summary>Get the number of bytes that have written to disk</summary>
		/// <returns>the number of bytes that have written to disk</returns>
		long GetBytesOnDisk();

		/// <summary>Get the number of bytes that are visible to readers</summary>
		/// <returns>the number of bytes that are visible to readers</returns>
		long GetVisibleLength();

		/// <summary>Return the storageUuid of the volume that stores this replica.</summary>
		string GetStorageUuid();

		/// <summary>Return true if the target volume is backed by RAM.</summary>
		bool IsOnTransientStorage();
	}
}

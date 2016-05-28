using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This class represents replicas being written.</summary>
	/// <remarks>
	/// This class represents replicas being written.
	/// Those are the replicas that
	/// are created in a pipeline initiated by a dfs client.
	/// </remarks>
	public class ReplicaBeingWritten : ReplicaInPipeline
	{
		/// <summary>Constructor for a zero length replica</summary>
		/// <param name="blockId">block id</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		/// <param name="bytesToReserve">
		/// disk space to reserve for this replica, based on
		/// the estimated maximum block length.
		/// </param>
		public ReplicaBeingWritten(long blockId, long genStamp, FsVolumeSpi vol, FilePath
			 dir, long bytesToReserve)
			: base(blockId, genStamp, vol, dir, bytesToReserve)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="block">a block</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		/// <param name="writer">a thread that is writing to this replica</param>
		public ReplicaBeingWritten(Block block, FsVolumeSpi vol, FilePath dir, Sharpen.Thread
			 writer)
			: base(block, vol, dir, writer)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="blockId">block id</param>
		/// <param name="len">replica length</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		/// <param name="writer">a thread that is writing to this replica</param>
		/// <param name="bytesToReserve">
		/// disk space to reserve for this replica, based on
		/// the estimated maximum block length.
		/// </param>
		public ReplicaBeingWritten(long blockId, long len, long genStamp, FsVolumeSpi vol
			, FilePath dir, Sharpen.Thread writer, long bytesToReserve)
			: base(blockId, len, genStamp, vol, dir, writer, bytesToReserve)
		{
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="from">where to copy from</param>
		public ReplicaBeingWritten(Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaBeingWritten
			 from)
			: base(from)
		{
		}

		public override long GetVisibleLength()
		{
			return GetBytesAcked();
		}

		// all acked bytes are visible
		public override HdfsServerConstants.ReplicaState GetState()
		{
			//ReplicaInfo
			return HdfsServerConstants.ReplicaState.Rbw;
		}

		public override bool Equals(object o)
		{
			// Object
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			// Object
			return base.GetHashCode();
		}
	}
}

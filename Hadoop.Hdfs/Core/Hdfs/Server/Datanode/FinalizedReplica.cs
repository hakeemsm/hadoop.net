using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This class describes a replica that has been finalized.</summary>
	public class FinalizedReplica : ReplicaInfo
	{
		private bool unlinked;

		/// <summary>Constructor</summary>
		/// <param name="blockId">block id</param>
		/// <param name="len">replica length</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		public FinalizedReplica(long blockId, long len, long genStamp, FsVolumeSpi vol, FilePath
			 dir)
			: base(blockId, len, genStamp, vol, dir)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="block">a block</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		public FinalizedReplica(Block block, FsVolumeSpi vol, FilePath dir)
			: base(block, vol, dir)
		{
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="from">where to copy construct from</param>
		public FinalizedReplica(Org.Apache.Hadoop.Hdfs.Server.Datanode.FinalizedReplica from
			)
			: base(from)
		{
			// copy-on-write done for block
			this.unlinked = from.IsUnlinked();
		}

		public override HdfsServerConstants.ReplicaState GetState()
		{
			// ReplicaInfo
			return HdfsServerConstants.ReplicaState.Finalized;
		}

		public override bool IsUnlinked()
		{
			// ReplicaInfo
			return unlinked;
		}

		public override void SetUnlinked()
		{
			// ReplicaInfo
			unlinked = true;
		}

		public override long GetVisibleLength()
		{
			return GetNumBytes();
		}

		// all bytes are visible
		public override long GetBytesOnDisk()
		{
			return GetNumBytes();
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

		public override string ToString()
		{
			return base.ToString() + "\n  unlinked          =" + unlinked;
		}
	}
}

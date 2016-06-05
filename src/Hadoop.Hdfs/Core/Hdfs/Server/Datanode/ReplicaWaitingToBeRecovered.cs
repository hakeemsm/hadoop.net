using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This class represents a replica that is waiting to be recovered.</summary>
	/// <remarks>
	/// This class represents a replica that is waiting to be recovered.
	/// After a datanode restart, any replica in "rbw" directory is loaded
	/// as a replica waiting to be recovered.
	/// A replica waiting to be recovered does not provision read nor
	/// participates in any pipeline recovery. It will become outdated if its
	/// client continues to write or be recovered as a result of
	/// lease recovery.
	/// </remarks>
	public class ReplicaWaitingToBeRecovered : ReplicaInfo
	{
		private bool unlinked;

		/// <summary>Constructor</summary>
		/// <param name="blockId">block id</param>
		/// <param name="len">replica length</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		public ReplicaWaitingToBeRecovered(long blockId, long len, long genStamp, FsVolumeSpi
			 vol, FilePath dir)
			: base(blockId, len, genStamp, vol, dir)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="block">a block</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		public ReplicaWaitingToBeRecovered(Block block, FsVolumeSpi vol, FilePath dir)
			: base(block, vol, dir)
		{
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="from">where to copy from</param>
		public ReplicaWaitingToBeRecovered(Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaWaitingToBeRecovered
			 from)
			: base(from)
		{
			// copy-on-write done for block
			this.unlinked = from.IsUnlinked();
		}

		public override HdfsServerConstants.ReplicaState GetState()
		{
			//ReplicaInfo
			return HdfsServerConstants.ReplicaState.Rwr;
		}

		public override bool IsUnlinked()
		{
			//ReplicaInfo
			return unlinked;
		}

		public override void SetUnlinked()
		{
			//ReplicaInfo
			unlinked = true;
		}

		public override long GetVisibleLength()
		{
			//ReplicaInfo
			return -1;
		}

		//no bytes are visible
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
			return base.ToString() + "\n  unlinked=" + unlinked;
		}
	}
}

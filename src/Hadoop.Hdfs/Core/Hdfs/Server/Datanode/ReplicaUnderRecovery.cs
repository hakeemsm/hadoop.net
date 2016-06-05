using System;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This class represents replicas that are under block recovery
	/// It has a recovery id that is equal to the generation stamp
	/// that the replica will be bumped to after recovery
	/// The recovery id is used to handle multiple concurrent block recoveries.
	/// </summary>
	/// <remarks>
	/// This class represents replicas that are under block recovery
	/// It has a recovery id that is equal to the generation stamp
	/// that the replica will be bumped to after recovery
	/// The recovery id is used to handle multiple concurrent block recoveries.
	/// A recovery with higher recovery id preempts recoveries with a lower id.
	/// </remarks>
	public class ReplicaUnderRecovery : ReplicaInfo
	{
		private ReplicaInfo original;

		private long recoveryId;

		public ReplicaUnderRecovery(ReplicaInfo replica, long recoveryId)
			: base(replica, replica.GetVolume(), replica.GetDir())
		{
			// the original replica that needs to be recovered
			// recovery id; it is also the generation stamp 
			// that the replica will be bumped to after recovery
			if (replica.GetState() != HdfsServerConstants.ReplicaState.Finalized && replica.GetState
				() != HdfsServerConstants.ReplicaState.Rbw && replica.GetState() != HdfsServerConstants.ReplicaState
				.Rwr)
			{
				throw new ArgumentException("Cannot recover replica: " + replica);
			}
			this.original = replica;
			this.recoveryId = recoveryId;
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="from">where to copy from</param>
		public ReplicaUnderRecovery(Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaUnderRecovery
			 from)
			: base(from)
		{
			this.original = from.GetOriginalReplica();
			this.recoveryId = from.GetRecoveryID();
		}

		/// <summary>Get the recovery id</summary>
		/// <returns>the generation stamp that the replica will be bumped to</returns>
		public virtual long GetRecoveryID()
		{
			return recoveryId;
		}

		/// <summary>Set the recovery id</summary>
		/// <param name="recoveryId">the new recoveryId</param>
		public virtual void SetRecoveryID(long recoveryId)
		{
			if (recoveryId > this.recoveryId)
			{
				this.recoveryId = recoveryId;
			}
			else
			{
				throw new ArgumentException("The new rcovery id: " + recoveryId + " must be greater than the current one: "
					 + this.recoveryId);
			}
		}

		/// <summary>Get the original replica that's under recovery</summary>
		/// <returns>the original replica under recovery</returns>
		public virtual ReplicaInfo GetOriginalReplica()
		{
			return original;
		}

		public override bool IsUnlinked()
		{
			//ReplicaInfo
			return original.IsUnlinked();
		}

		public override void SetUnlinked()
		{
			//ReplicaInfo
			original.SetUnlinked();
		}

		public override HdfsServerConstants.ReplicaState GetState()
		{
			//ReplicaInfo
			return HdfsServerConstants.ReplicaState.Rur;
		}

		public override long GetVisibleLength()
		{
			return original.GetVisibleLength();
		}

		public override long GetBytesOnDisk()
		{
			return original.GetBytesOnDisk();
		}

		public override void SetBlockId(long blockId)
		{
			//org.apache.hadoop.hdfs.protocol.Block
			base.SetBlockId(blockId);
			original.SetBlockId(blockId);
		}

		public override void SetGenerationStamp(long gs)
		{
			//org.apache.hadoop.hdfs.protocol.Block
			base.SetGenerationStamp(gs);
			original.SetGenerationStamp(gs);
		}

		public override void SetNumBytes(long numBytes)
		{
			//org.apache.hadoop.hdfs.protocol.Block
			base.SetNumBytes(numBytes);
			original.SetNumBytes(numBytes);
		}

		public override void SetDir(FilePath dir)
		{
			//ReplicaInfo
			base.SetDir(dir);
			original.SetDir(dir);
		}

		internal override void SetVolume(FsVolumeSpi vol)
		{
			//ReplicaInfo
			base.SetVolume(vol);
			original.SetVolume(vol);
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
			return base.ToString() + "\n  recoveryId=" + recoveryId + "\n  original=" + original;
		}

		public virtual ReplicaRecoveryInfo CreateInfo()
		{
			return new ReplicaRecoveryInfo(original.GetBlockId(), original.GetBytesOnDisk(), 
				original.GetGenerationStamp(), original.GetState());
		}
	}
}

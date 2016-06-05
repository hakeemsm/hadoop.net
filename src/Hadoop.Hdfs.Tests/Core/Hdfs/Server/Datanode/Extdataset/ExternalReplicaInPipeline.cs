using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Extdataset
{
	public class ExternalReplicaInPipeline : ReplicaInPipelineInterface
	{
		public virtual void SetNumBytes(long bytesReceived)
		{
		}

		public virtual long GetBytesAcked()
		{
			return 0;
		}

		public virtual void SetBytesAcked(long bytesAcked)
		{
		}

		public virtual void ReleaseAllBytesReserved()
		{
		}

		public virtual void SetLastChecksumAndDataLen(long dataLength, byte[] lastChecksum
			)
		{
		}

		public virtual ChunkChecksum GetLastChecksumAndDataLen()
		{
			return new ChunkChecksum(0, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ReplicaOutputStreams CreateStreams(bool isCreate, DataChecksum requestedChecksum
			)
		{
			return new ReplicaOutputStreams(null, null, requestedChecksum, false);
		}

		public virtual long GetBlockId()
		{
			return 0;
		}

		public virtual long GetGenerationStamp()
		{
			return 0;
		}

		public virtual HdfsServerConstants.ReplicaState GetState()
		{
			return HdfsServerConstants.ReplicaState.Finalized;
		}

		public virtual long GetNumBytes()
		{
			return 0;
		}

		public virtual long GetBytesOnDisk()
		{
			return 0;
		}

		public virtual long GetVisibleLength()
		{
			return 0;
		}

		public virtual string GetStorageUuid()
		{
			return null;
		}

		public virtual bool IsOnTransientStorage()
		{
			return false;
		}
	}
}

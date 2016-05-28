using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Extdataset
{
	public class ExternalReplica : Replica
	{
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

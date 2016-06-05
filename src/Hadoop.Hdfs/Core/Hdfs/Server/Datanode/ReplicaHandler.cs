using System;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This class includes a replica being actively written and the reference to
	/// the fs volume where this replica is located.
	/// </summary>
	public class ReplicaHandler : IDisposable
	{
		private readonly ReplicaInPipelineInterface replica;

		private readonly FsVolumeReference volumeReference;

		public ReplicaHandler(ReplicaInPipelineInterface replica, FsVolumeReference reference
			)
		{
			this.replica = replica;
			this.volumeReference = reference;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (this.volumeReference != null)
			{
				volumeReference.Close();
			}
		}

		public virtual ReplicaInPipelineInterface GetReplica()
		{
			return replica;
		}
	}
}

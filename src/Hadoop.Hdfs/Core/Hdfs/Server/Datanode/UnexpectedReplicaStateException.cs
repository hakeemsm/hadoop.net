using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Exception indicating that the replica is in an unexpected state</summary>
	[System.Serializable]
	public class UnexpectedReplicaStateException : IOException
	{
		private const long serialVersionUID = 1L;

		public UnexpectedReplicaStateException()
			: base()
		{
		}

		public UnexpectedReplicaStateException(ExtendedBlock b, HdfsServerConstants.ReplicaState
			 expectedState)
			: base("Replica " + b + " is not in expected state " + expectedState)
		{
		}

		public UnexpectedReplicaStateException(string msg)
			: base(msg)
		{
		}
	}
}

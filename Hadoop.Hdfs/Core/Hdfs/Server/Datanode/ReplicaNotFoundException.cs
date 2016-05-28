using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Exception indicating that DataNode does not have a replica
	/// that matches the target block.
	/// </summary>
	[System.Serializable]
	public class ReplicaNotFoundException : IOException
	{
		private const long serialVersionUID = 1L;

		public const string NonRbwReplica = "Cannot recover a non-RBW replica ";

		public const string UnfinalizedReplica = "Cannot append to an unfinalized replica ";

		public const string UnfinalizedAndNonrbwReplica = "Cannot recover append/close to a replica that's not FINALIZED and not RBW ";

		public const string NonExistentReplica = "Cannot append to a non-existent replica ";

		public const string UnexpectedGsReplica = "Cannot append to a replica with unexpected generation stamp ";

		public ReplicaNotFoundException()
			: base()
		{
		}

		public ReplicaNotFoundException(ExtendedBlock b)
			: base("Replica not found for " + b)
		{
		}

		public ReplicaNotFoundException(string msg)
			: base(msg)
		{
		}
	}
}

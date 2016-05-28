using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Exception indicating that the target block already exists
	/// and is not set to be recovered/overwritten.
	/// </summary>
	[System.Serializable]
	public class ReplicaAlreadyExistsException : IOException
	{
		private const long serialVersionUID = 1L;

		public ReplicaAlreadyExistsException()
			: base()
		{
		}

		public ReplicaAlreadyExistsException(string msg)
			: base(msg)
		{
		}
	}
}

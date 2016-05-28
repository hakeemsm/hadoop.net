using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// This exception is thrown when a datanode tries to register or communicate
	/// with the namenode when it does not appear on the list of included nodes,
	/// or has been specifically excluded.
	/// </summary>
	[System.Serializable]
	public class DisallowedDatanodeException : IOException
	{
		/// <summary>for java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		public DisallowedDatanodeException(DatanodeID nodeID, string reason)
			: base("Datanode denied communication with namenode because " + reason + ": " + nodeID
				)
		{
		}

		public DisallowedDatanodeException(DatanodeID nodeID)
			: this(nodeID, "the host is not in the include-list")
		{
		}
	}
}

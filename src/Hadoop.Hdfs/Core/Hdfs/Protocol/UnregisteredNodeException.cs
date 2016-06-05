using System.IO;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// This exception is thrown when a node that has not previously
	/// registered is trying to access the name node.
	/// </summary>
	[System.Serializable]
	public class UnregisteredNodeException : IOException
	{
		private const long serialVersionUID = -5620209396945970810L;

		public UnregisteredNodeException(JournalInfo info)
			: base("Unregistered server: " + info.ToString())
		{
		}

		public UnregisteredNodeException(NodeRegistration nodeReg)
			: base("Unregistered server: " + nodeReg.ToString())
		{
		}

		/// <summary>
		/// The exception is thrown if a different data-node claims the same
		/// storage id as the existing one.
		/// </summary>
		/// <param name="nodeID">unregistered data-node</param>
		/// <param name="storedNode">data-node stored in the system with this storage id</param>
		public UnregisteredNodeException(DatanodeID nodeID, DatanodeInfo storedNode)
			: base("Data node " + nodeID + " is attempting to report storage ID " + nodeID.GetDatanodeUuid
				() + ". Node " + storedNode + " is expected to serve this storage.")
		{
		}
	}
}

using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>An inter-datanode protocol for updating generation stamp</summary>
	public abstract class InterDatanodeProtocol
	{
		public const Log Log = LogFactory.GetLog(typeof(InterDatanodeProtocol));

		/// <summary>
		/// Until version 9, this class InterDatanodeProtocol served as both
		/// the interface to the DN AND the RPC protocol used to communicate with the
		/// DN.
		/// </summary>
		/// <remarks>
		/// Until version 9, this class InterDatanodeProtocol served as both
		/// the interface to the DN AND the RPC protocol used to communicate with the
		/// DN.
		/// This class is used by both the DN to insulate from the protocol
		/// serialization.
		/// If you are adding/changing DN's interface then you need to
		/// change both this class and ALSO related protocol buffer
		/// wire protocol definition in InterDatanodeProtocol.proto.
		/// For more details on protocol buffer wire protocol, please see
		/// .../org/apache/hadoop/hdfs/protocolPB/overview.html
		/// </remarks>
		public const long versionID = 6L;

		/// <summary>Initialize a replica recovery.</summary>
		/// <returns>
		/// actual state of the replica on this data-node or
		/// null if data-node does not have the replica.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock);

		/// <summary>Update replica with the new generation stamp and length.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newLength);
	}

	public static class InterDatanodeProtocolConstants
	{
	}
}

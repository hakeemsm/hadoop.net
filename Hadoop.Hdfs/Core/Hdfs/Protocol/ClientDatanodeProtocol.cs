using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>An client-datanode protocol for block recovery</summary>
	public abstract class ClientDatanodeProtocol
	{
		/// <summary>
		/// Until version 9, this class ClientDatanodeProtocol served as both
		/// the client interface to the DN AND the RPC protocol used to
		/// communicate with the NN.
		/// </summary>
		/// <remarks>
		/// Until version 9, this class ClientDatanodeProtocol served as both
		/// the client interface to the DN AND the RPC protocol used to
		/// communicate with the NN.
		/// This class is used by both the DFSClient and the
		/// DN server side to insulate from the protocol serialization.
		/// If you are adding/changing DN's interface then you need to
		/// change both this class and ALSO related protocol buffer
		/// wire protocol definition in ClientDatanodeProtocol.proto.
		/// For more details on protocol buffer wire protocol, please see
		/// .../org/apache/hadoop/hdfs/protocolPB/overview.html
		/// The log of historical changes can be retrieved from the svn).
		/// 9: Added deleteBlockPool method
		/// 9 is the last version id when this class was used for protocols
		/// serialization. DO not update this version any further.
		/// </remarks>
		public const long versionID = 9L;

		/// <summary>Return the visible length of a replica.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract long GetReplicaVisibleLength(ExtendedBlock b);

		/// <summary>
		/// Refresh the list of federated namenodes from updated configuration
		/// Adds new namenodes and stops the deleted namenodes.
		/// </summary>
		/// <exception cref="System.IO.IOException">on error</exception>
		public abstract void RefreshNamenodes();

		/// <summary>Delete the block pool directory.</summary>
		/// <remarks>
		/// Delete the block pool directory. If force is false it is deleted only if
		/// it is empty, otherwise it is deleted along with its contents.
		/// </remarks>
		/// <param name="bpid">Blockpool id to be deleted.</param>
		/// <param name="force">
		/// If false blockpool directory is deleted only if it is empty
		/// i.e. if it doesn't contain any block files, otherwise it is
		/// deleted along with its contents.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DeleteBlockPool(string bpid, bool force);

		/// <summary>
		/// Retrieves the path names of the block file and metadata file stored on the
		/// local file system.
		/// </summary>
		/// <remarks>
		/// Retrieves the path names of the block file and metadata file stored on the
		/// local file system.
		/// In order for this method to work, one of the following should be satisfied:
		/// <ul>
		/// <li>
		/// The client user must be configured at the datanode to be able to use this
		/// method.</li>
		/// <li>
		/// When security is enabled, kerberos authentication must be used to connect
		/// to the datanode.</li>
		/// </ul>
		/// </remarks>
		/// <param name="block">the specified block on the local datanode</param>
		/// <param name="token">the block access token.</param>
		/// <returns>the BlockLocalPathInfo of a block</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public abstract BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token);

		/// <summary>Retrieves volume location information about a list of blocks on a datanode.
		/// 	</summary>
		/// <remarks>
		/// Retrieves volume location information about a list of blocks on a datanode.
		/// This is in the form of an opaque
		/// <see cref="Org.Apache.Hadoop.FS.VolumeId"/>
		/// for each configured data directory, which is not guaranteed to be
		/// the same across DN restarts.
		/// </remarks>
		/// <param name="blockPoolId">the pool to query</param>
		/// <param name="blockIds">list of blocks on the local datanode</param>
		/// <param name="tokens">block access tokens corresponding to the requested blocks</param>
		/// <returns>
		/// an HdfsBlocksMetadata that associates
		/// <see cref="ExtendedBlock"/>
		/// s with
		/// data directories
		/// </returns>
		/// <exception cref="System.IO.IOException">if datanode is unreachable, or replica is not found on datanode
		/// 	</exception>
		public abstract HdfsBlocksMetadata GetHdfsBlocksMetadata(string blockPoolId, long
			[] blockIds, IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>>
			 tokens);

		/// <summary>Shuts down a datanode.</summary>
		/// <param name="forUpgrade">
		/// If true, data node does extra prep work before shutting
		/// down. The work includes advising clients to wait and saving
		/// certain states for quick restart. This should only be used when
		/// the stored data will remain the same during upgrade/restart.
		/// </param>
		/// <exception cref="System.IO.IOException"></exception>
		public abstract void ShutdownDatanode(bool forUpgrade);

		/// <summary>Obtains datanode info</summary>
		/// <returns>software/config version and uptime of the datanode</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract DatanodeLocalInfo GetDatanodeInfo();

		/// <summary>Asynchronously reload configuration on disk and apply changes.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StartReconfiguration();

		/// <summary>Get the status of the previously issued reconfig task.</summary>
		/// <seealso>
		/// 
		/// <see cref="Org.Apache.Hadoop.Conf.ReconfigurationTaskStatus"/>
		/// .
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReconfigurationTaskStatus GetReconfigurationStatus();

		/// <summary>Trigger a new block report.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void TriggerBlockReport(BlockReportOptions options);
	}

	public static class ClientDatanodeProtocolConstants
	{
	}
}

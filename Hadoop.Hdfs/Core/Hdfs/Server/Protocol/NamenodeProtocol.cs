using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Protocol that a secondary NameNode uses to communicate with the NameNode.
	/// 	</summary>
	/// <remarks>
	/// Protocol that a secondary NameNode uses to communicate with the NameNode.
	/// It's used to get part of the name node state
	/// </remarks>
	public abstract class NamenodeProtocol
	{
		/// <summary>
		/// Until version 6L, this class served as both
		/// the client interface to the NN AND the RPC protocol used to
		/// communicate with the NN.
		/// </summary>
		/// <remarks>
		/// Until version 6L, this class served as both
		/// the client interface to the NN AND the RPC protocol used to
		/// communicate with the NN.
		/// This class is used by both the DFSClient and the
		/// NN server side to insulate from the protocol serialization.
		/// If you are adding/changing NN's interface then you need to
		/// change both this class and ALSO related protocol buffer
		/// wire protocol definition in NamenodeProtocol.proto.
		/// For more details on protocol buffer wire protocol, please see
		/// .../org/apache/hadoop/hdfs/protocolPB/overview.html
		/// 6: Switch to txid-based file naming for image and edits
		/// </remarks>
		public const long versionID = 6L;

		public const int Notify = 0;

		public const int Fatal = 1;

		public const int ActUnknown = 0;

		public const int ActShutdown = 50;

		public const int ActCheckpoint = 51;

		// Error codes passed by errorReport().
		// unknown action   
		// shutdown node
		// do checkpoint
		/// <summary>
		/// Get a list of blocks belonging to <code>datanode</code>
		/// whose total size equals <code>size</code>.
		/// </summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer"/>
		/// <param name="datanode">a data node</param>
		/// <param name="size">requested size</param>
		/// <returns>a list of blocks & their locations</returns>
		/// <exception cref="System.IO.IOException">
		/// if size is less than or equal to 0 or
		/// datanode does not exist
		/// </exception>
		[Idempotent]
		public abstract BlocksWithLocations GetBlocks(DatanodeInfo datanode, long size);

		/// <summary>Get the current block keys</summary>
		/// <returns>ExportedBlockKeys containing current block keys</returns>
		/// <exception cref="System.IO.IOException"></exception>
		[Idempotent]
		public abstract ExportedBlockKeys GetBlockKeys();

		/// <returns>
		/// The most recent transaction ID that has been synced to
		/// persistent storage, or applied from persistent storage in the
		/// case of a non-active node.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract long GetTransactionID();

		/// <summary>Get the transaction ID of the most recent checkpoint.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract long GetMostRecentCheckpointTxId();

		/// <summary>Closes the current edit log and opens a new one.</summary>
		/// <remarks>
		/// Closes the current edit log and opens a new one. The
		/// call fails if the file system is in SafeMode.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>a unique token to identify this transaction.</returns>
		[Idempotent]
		public abstract CheckpointSignature RollEditLog();

		/// <summary>Request name-node version and storage information.</summary>
		/// <returns>
		/// 
		/// <see cref="NamespaceInfo"/>
		/// identifying versions and storage information
		/// of the name-node
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract NamespaceInfo VersionRequest();

		/// <summary>Report to the active name-node an error occurred on a subordinate node.</summary>
		/// <remarks>
		/// Report to the active name-node an error occurred on a subordinate node.
		/// Depending on the error code the active node may decide to unregister the
		/// reporting node.
		/// </remarks>
		/// <param name="registration">requesting node.</param>
		/// <param name="errorCode">indicates the error</param>
		/// <param name="msg">free text description of the error</param>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void ErrorReport(NamenodeRegistration registration, int errorCode
			, string msg);

		/// <summary>Register a subordinate name-node like backup node.</summary>
		/// <returns>
		/// 
		/// <see cref="NamenodeRegistration"/>
		/// of the node,
		/// which this node has just registered with.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract NamenodeRegistration RegisterSubordinateNamenode(NamenodeRegistration
			 registration);

		/// <summary>A request to the active name-node to start a checkpoint.</summary>
		/// <remarks>
		/// A request to the active name-node to start a checkpoint.
		/// The name-node should decide whether to admit it or reject.
		/// The name-node also decides what should be done with the backup node
		/// image before and after the checkpoint.
		/// </remarks>
		/// <seealso cref="CheckpointCommand"/>
		/// <seealso cref="NamenodeCommand"/>
		/// <seealso cref="ActShutdown"/>
		/// <param name="registration">the requesting node</param>
		/// <returns>
		/// 
		/// <see cref="CheckpointCommand"/>
		/// if checkpoint is allowed.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract NamenodeCommand StartCheckpoint(NamenodeRegistration registration
			);

		/// <summary>
		/// A request to the active name-node to finalize
		/// previously started checkpoint.
		/// </summary>
		/// <param name="registration">the requesting node</param>
		/// <param name="sig">
		/// 
		/// <c>CheckpointSignature</c>
		/// which identifies the checkpoint.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract void EndCheckpoint(NamenodeRegistration registration, CheckpointSignature
			 sig);

		/// <summary>
		/// Return a structure containing details about all edit logs
		/// available to be fetched from the NameNode.
		/// </summary>
		/// <param name="sinceTxId">return only logs that contain transactions &gt;= sinceTxId
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract RemoteEditLogManifest GetEditLogManifest(long sinceTxId);

		/// <returns>Whether the NameNode is in upgrade state (false) or not (true)</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract bool IsUpgradeFinalized();
	}

	public static class NamenodeProtocolConstants
	{
	}
}

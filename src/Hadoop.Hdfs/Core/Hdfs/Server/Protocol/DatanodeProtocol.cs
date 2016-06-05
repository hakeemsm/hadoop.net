using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Protocol that a DFS datanode uses to communicate with the NameNode.</summary>
	/// <remarks>
	/// Protocol that a DFS datanode uses to communicate with the NameNode.
	/// It's used to upload current load information and block reports.
	/// The only way a NameNode can communicate with a DataNode is by
	/// returning values from these functions.
	/// </remarks>
	public abstract class DatanodeProtocol
	{
		/// <summary>
		/// This class is used by both the Namenode (client) and BackupNode (server)
		/// to insulate from the protocol serialization.
		/// </summary>
		/// <remarks>
		/// This class is used by both the Namenode (client) and BackupNode (server)
		/// to insulate from the protocol serialization.
		/// If you are adding/changing DN's interface then you need to
		/// change both this class and ALSO related protocol buffer
		/// wire protocol definition in DatanodeProtocol.proto.
		/// For more details on protocol buffer wire protocol, please see
		/// .../org/apache/hadoop/hdfs/protocolPB/overview.html
		/// </remarks>
		public const long versionID = 28L;

		public const int Notify = 0;

		public const int DiskError = 1;

		public const int InvalidBlock = 2;

		public const int FatalDiskError = 3;

		/// <summary>
		/// Determines actions that data node should perform
		/// when receiving a datanode command.
		/// </summary>
		public const int DnaUnknown = 0;

		public const int DnaTransfer = 1;

		public const int DnaInvalidate = 2;

		public const int DnaShutdown = 3;

		public const int DnaRegister = 4;

		public const int DnaFinalize = 5;

		public const int DnaRecoverblock = 6;

		public const int DnaAccesskeyupdate = 7;

		public const int DnaBalancerbandwidthupdate = 8;

		public const int DnaCache = 9;

		public const int DnaUncache = 10;

		// error code
		// there are still valid volumes on DN
		// no valid volumes left on DN
		// unknown action   
		// transfer blocks to another datanode
		// invalidate blocks
		// shutdown node
		// re-register
		// finalize previous upgrade
		// request a block recovery
		// update access key
		// update balancer bandwidth
		// cache blocks
		// uncache blocks
		/// <summary>Register Datanode.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem.RegisterDatanode(DatanodeRegistration)
		/// 	"/>
		/// <param name="registration">datanode registration information</param>
		/// <returns>
		/// the given
		/// <see cref="DatanodeRegistration"/>
		/// with
		/// updated registration information
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract DatanodeRegistration RegisterDatanode(DatanodeRegistration registration
			);

		/// <summary>
		/// sendHeartbeat() tells the NameNode that the DataNode is still
		/// alive and well.
		/// </summary>
		/// <remarks>
		/// sendHeartbeat() tells the NameNode that the DataNode is still
		/// alive and well.  Includes some status info, too.
		/// It also gives the NameNode a chance to return
		/// an array of "DatanodeCommand" objects in HeartbeatResponse.
		/// A DatanodeCommand tells the DataNode to invalidate local block(s),
		/// or to copy them to other DataNodes, etc.
		/// </remarks>
		/// <param name="registration">datanode registration information</param>
		/// <param name="reports">utilization report per storage</param>
		/// <param name="xmitsInProgress">number of transfers from this datanode to others</param>
		/// <param name="xceiverCount">number of active transceiver threads</param>
		/// <param name="failedVolumes">number of failed volumes</param>
		/// <param name="volumeFailureSummary">info about volume failures</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		[Idempotent]
		public abstract HeartbeatResponse SendHeartbeat(DatanodeRegistration registration
			, StorageReport[] reports, long dnCacheCapacity, long dnCacheUsed, int xmitsInProgress
			, int xceiverCount, int failedVolumes, VolumeFailureSummary volumeFailureSummary
			);

		/// <summary>blockReport() tells the NameNode about all the locally-stored blocks.</summary>
		/// <remarks>
		/// blockReport() tells the NameNode about all the locally-stored blocks.
		/// The NameNode returns an array of Blocks that have become obsolete
		/// and should be deleted.  This function is meant to upload *all
		/// the locally-stored blocks.  It's invoked upon startup and then
		/// infrequently afterwards.
		/// </remarks>
		/// <param name="registration">datanode registration</param>
		/// <param name="poolId">the block pool ID for the blocks</param>
		/// <param name="reports">
		/// report of blocks per storage
		/// Each finalized block is represented as 3 longs. Each under-
		/// construction replica is represented as 4 longs.
		/// This is done instead of Block[] to reduce memory used by block reports.
		/// </param>
		/// <param name="reports">report of blocks per storage</param>
		/// <param name="context">Context information for this block report.</param>
		/// <returns>- the next command for DN to process.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract DatanodeCommand BlockReport(DatanodeRegistration registration, string
			 poolId, StorageBlockReport[] reports, BlockReportContext context);

		/// <summary>Communicates the complete list of locally cached blocks to the NameNode.
		/// 	</summary>
		/// <remarks>
		/// Communicates the complete list of locally cached blocks to the NameNode.
		/// This method is similar to
		/// <see cref="BlockReport(DatanodeRegistration, string, StorageBlockReport[], BlockReportContext)
		/// 	"/>
		/// ,
		/// which is used to communicated blocks stored on disk.
		/// </remarks>
		/// <param name="The">datanode registration.</param>
		/// <param name="poolId">The block pool ID for the blocks.</param>
		/// <param name="blockIds">A list of block IDs.</param>
		/// <returns>The DatanodeCommand.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract DatanodeCommand CacheReport(DatanodeRegistration registration, string
			 poolId, IList<long> blockIds);

		/// <summary>
		/// blockReceivedAndDeleted() allows the DataNode to tell the NameNode about
		/// recently-received and -deleted block data.
		/// </summary>
		/// <remarks>
		/// blockReceivedAndDeleted() allows the DataNode to tell the NameNode about
		/// recently-received and -deleted block data.
		/// For the case of received blocks, a hint for preferred replica to be
		/// deleted when there is any excessive blocks is provided.
		/// For example, whenever client code
		/// writes a new Block here, or another DataNode copies a Block to
		/// this DataNode, it will call blockReceived().
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void BlockReceivedAndDeleted(DatanodeRegistration registration, string
			 poolId, StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks);

		/// <summary>
		/// errorReport() tells the NameNode about something that has gone
		/// awry.
		/// </summary>
		/// <remarks>
		/// errorReport() tells the NameNode about something that has gone
		/// awry.  Useful for debugging.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void ErrorReport(DatanodeRegistration registration, int errorCode
			, string msg);

		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract NamespaceInfo VersionRequest();

		/// <summary>
		/// same as
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.ReportBadBlocks(Org.Apache.Hadoop.Hdfs.Protocol.LocatedBlock[])
		/// 	"/>
		/// }
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void ReportBadBlocks(LocatedBlock[] blocks);

		/// <summary>Commit block synchronization in lease recovery</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void CommitBlockSynchronization(ExtendedBlock block, long newgenerationstamp
			, long newlength, bool closeFile, bool deleteblock, DatanodeID[] newtargets, string
			[] newtargetstorages);
	}

	public static class DatanodeProtocolConstants
	{
	}
}

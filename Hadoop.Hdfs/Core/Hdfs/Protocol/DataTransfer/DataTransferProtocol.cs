using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Transfer data to/from datanode using a streaming protocol.</summary>
	public abstract class DataTransferProtocol
	{
		public const Log Log = LogFactory.GetLog(typeof(DataTransferProtocol));

		/// <summary>
		/// Version for data transfers between clients and datanodes
		/// This should change when serialization of DatanodeInfo, not just
		/// when protocol changes.
		/// </summary>
		/// <remarks>
		/// Version for data transfers between clients and datanodes
		/// This should change when serialization of DatanodeInfo, not just
		/// when protocol changes. It is not very obvious.
		/// </remarks>
		public const int DataTransferVersion = 28;

		/*
		* Version 28:
		*    Declare methods in DataTransferProtocol interface.
		*/
		/// <summary>Read a block.</summary>
		/// <param name="blk">the block being read.</param>
		/// <param name="blockToken">security token for accessing the block.</param>
		/// <param name="clientName">client's name.</param>
		/// <param name="blockOffset">offset of the block.</param>
		/// <param name="length">maximum number of bytes for this read.</param>
		/// <param name="sendChecksum">
		/// if false, the DN should skip reading and sending
		/// checksums
		/// </param>
		/// <param name="cachingStrategy">The caching strategy to use.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ReadBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, long blockOffset, long length
			, bool sendChecksum, CachingStrategy cachingStrategy);

		/// <summary>Write a block to a datanode pipeline.</summary>
		/// <remarks>
		/// Write a block to a datanode pipeline.
		/// The receiver datanode of this call is the next datanode in the pipeline.
		/// The other downstream datanodes are specified by the targets parameter.
		/// Note that the receiver
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo"/>
		/// is not required in the
		/// parameter list since the receiver datanode knows its info.  However, the
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// for storing the replica in the receiver datanode is a
		/// parameter since the receiver datanode may support multiple storage types.
		/// </remarks>
		/// <param name="blk">the block being written.</param>
		/// <param name="storageType">for storing the replica in the receiver datanode.</param>
		/// <param name="blockToken">security token for accessing the block.</param>
		/// <param name="clientName">client's name.</param>
		/// <param name="targets">other downstream datanodes in the pipeline.</param>
		/// <param name="targetStorageTypes">
		/// target
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// s corresponding
		/// to the target datanodes.
		/// </param>
		/// <param name="source">source datanode.</param>
		/// <param name="stage">pipeline stage.</param>
		/// <param name="pipelineSize">the size of the pipeline.</param>
		/// <param name="minBytesRcvd">minimum number of bytes received.</param>
		/// <param name="maxBytesRcvd">maximum number of bytes received.</param>
		/// <param name="latestGenerationStamp">the latest generation stamp of the block.</param>
		/// <param name="pinning">whether to pin the block, so Balancer won't move it.</param>
		/// <param name="targetPinnings">whether to pin the block on target datanode</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void WriteBlock(ExtendedBlock blk, StorageType storageType, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, DatanodeInfo[] targets, StorageType
			[] targetStorageTypes, DatanodeInfo source, BlockConstructionStage stage, int pipelineSize
			, long minBytesRcvd, long maxBytesRcvd, long latestGenerationStamp, DataChecksum
			 requestedChecksum, CachingStrategy cachingStrategy, bool allowLazyPersist, bool
			 pinning, bool[] targetPinnings);

		/// <summary>Transfer a block to another datanode.</summary>
		/// <remarks>
		/// Transfer a block to another datanode.
		/// The block stage must be
		/// either
		/// <see cref="BlockConstructionStage.TransferRbw"/>
		/// or
		/// <see cref="BlockConstructionStage.TransferFinalized"/>
		/// .
		/// </remarks>
		/// <param name="blk">the block being transferred.</param>
		/// <param name="blockToken">security token for accessing the block.</param>
		/// <param name="clientName">client's name.</param>
		/// <param name="targets">target datanodes.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void TransferBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, DatanodeInfo[] targets, StorageType
			[] targetStorageTypes);

		/// <summary>Request short circuit access file descriptors from a DataNode.</summary>
		/// <param name="blk">The block to get file descriptors for.</param>
		/// <param name="blockToken">Security token for accessing the block.</param>
		/// <param name="slotId">
		/// The shared memory slot id to use, or null
		/// to use no slot id.
		/// </param>
		/// <param name="maxVersion">
		/// Maximum version of the block data the client
		/// can understand.
		/// </param>
		/// <param name="supportsReceiptVerification">
		/// True if the client supports
		/// receipt verification.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RequestShortCircuitFds(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, ShortCircuitShm.SlotId slotId, int maxVersion
			, bool supportsReceiptVerification);

		/// <summary>Release a pair of short-circuit FDs requested earlier.</summary>
		/// <param name="slotId">SlotID used by the earlier file descriptors.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ReleaseShortCircuitFds(ShortCircuitShm.SlotId slotId);

		/// <summary>Request a short circuit shared memory area from a DataNode.</summary>
		/// <param name="clientName">The name of the client.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RequestShortCircuitShm(string clientName);

		/// <summary>
		/// Receive a block from a source datanode
		/// and then notifies the namenode
		/// to remove the copy from the original datanode.
		/// </summary>
		/// <remarks>
		/// Receive a block from a source datanode
		/// and then notifies the namenode
		/// to remove the copy from the original datanode.
		/// Note that the source datanode and the original datanode can be different.
		/// It is used for balancing purpose.
		/// </remarks>
		/// <param name="blk">the block being replaced.</param>
		/// <param name="storageType">
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// for storing the block.
		/// </param>
		/// <param name="blockToken">security token for accessing the block.</param>
		/// <param name="delHint">the hint for deleting the block in the original datanode.</param>
		/// <param name="source">the source datanode for receiving the block.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ReplaceBlock(ExtendedBlock blk, StorageType storageType, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string delHint, DatanodeInfo source);

		/// <summary>Copy a block.</summary>
		/// <remarks>
		/// Copy a block.
		/// It is used for balancing purpose.
		/// </remarks>
		/// <param name="blk">the block being copied.</param>
		/// <param name="blockToken">security token for accessing the block.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void CopyBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken);

		/// <summary>Get block checksum (MD5 of CRC32).</summary>
		/// <param name="blk">a block.</param>
		/// <param name="blockToken">security token for accessing the block.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void BlockChecksum(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken);
	}

	public static class DataTransferProtocolConstants
	{
	}
}

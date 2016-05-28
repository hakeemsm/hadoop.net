using System.Collections.Generic;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// ClientProtocol is used by user code via
	/// <see cref="Org.Apache.Hadoop.Hdfs.DistributedFileSystem"/>
	/// class to communicate
	/// with the NameNode.  User code can manipulate the directory namespace,
	/// as well as open/close file streams, etc.
	/// </summary>
	public abstract class ClientProtocol
	{
		/// <summary>
		/// Until version 69, this class ClientProtocol served as both
		/// the client interface to the NN AND the RPC protocol used to
		/// communicate with the NN.
		/// </summary>
		/// <remarks>
		/// Until version 69, this class ClientProtocol served as both
		/// the client interface to the NN AND the RPC protocol used to
		/// communicate with the NN.
		/// This class is used by both the DFSClient and the
		/// NN server side to insulate from the protocol serialization.
		/// If you are adding/changing this interface then you need to
		/// change both this class and ALSO related protocol buffer
		/// wire protocol definition in ClientNamenodeProtocol.proto.
		/// For more details on protocol buffer wire protocol, please see
		/// .../org/apache/hadoop/hdfs/protocolPB/overview.html
		/// The log of historical changes can be retrieved from the svn).
		/// 69: Eliminate overloaded method names.
		/// 69L is the last version id when this class was used for protocols
		/// serialization. DO not update this version any further.
		/// </remarks>
		public const long versionID = 69L;

		///////////////////////////////////////
		// File contents
		///////////////////////////////////////
		/// <summary>Get locations of the blocks of the specified file within the specified range.
		/// 	</summary>
		/// <remarks>
		/// Get locations of the blocks of the specified file within the specified range.
		/// DataNode locations for each block are sorted by
		/// the proximity to the client.
		/// <p>
		/// Return
		/// <see cref="LocatedBlocks"/>
		/// which contains
		/// file length, blocks and their locations.
		/// DataNode locations for each block are sorted by
		/// the distance to the client's address.
		/// <p>
		/// The client will then have to contact
		/// one of the indicated DataNodes to obtain the actual data.
		/// </remarks>
		/// <param name="src">file name</param>
		/// <param name="offset">range start offset</param>
		/// <param name="length">range length</param>
		/// <returns>file length and array of blocks with their locations</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> does not exist
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract LocatedBlocks GetBlockLocations(string src, long offset, long length
			);

		/// <summary>Get server default values for a number of configuration params.</summary>
		/// <returns>a set of server default configuration values</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract FsServerDefaults GetServerDefaults();

		/// <summary>Create a new file entry in the namespace.</summary>
		/// <remarks>
		/// Create a new file entry in the namespace.
		/// <p>
		/// This will create an empty file specified by the source path.
		/// The path should reflect a full path originated at the root.
		/// The name-node does not have a notion of "current" directory for a client.
		/// <p>
		/// Once created, the file is visible and available for read to other clients.
		/// Although, other clients cannot
		/// <see cref="Delete(string, bool)"/>
		/// , re-create or
		/// <see cref="Rename(string, string)"/>
		/// it until the file is completed
		/// or explicitly as a result of lease expiration.
		/// <p>
		/// Blocks have a maximum size.  Clients that intend to create
		/// multi-block files must also use
		/// <see cref="AddBlock(string, string, ExtendedBlock, DatanodeInfo[], long, string[])
		/// 	"/>
		/// </remarks>
		/// <param name="src">path of the file being created.</param>
		/// <param name="masked">masked permission.</param>
		/// <param name="clientName">name of the current client.</param>
		/// <param name="flag">
		/// indicates whether the file should be
		/// overwritten if it already exists or create if it does not exist or append.
		/// </param>
		/// <param name="createParent">create missing parent directory if true</param>
		/// <param name="replication">block replication factor.</param>
		/// <param name="blockSize">maximum block size.</param>
		/// <param name="supportedVersions">CryptoProtocolVersions supported by the client</param>
		/// <returns>
		/// the status of the created file, it could be null if the server
		/// doesn't support returning the file status
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="AlreadyBeingCreatedException">if the path does not exist.</exception>
		/// <exception cref="DSQuotaExceededException">
		/// If file creation violates disk space
		/// quota restriction
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException">If file <code>src</code> already exists
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">
		/// If parent of <code>src</code> does not exist
		/// and <code>createParent</code> is false
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException">
		/// If parent of <code>src</code> is not a
		/// directory.
		/// </exception>
		/// <exception cref="NSQuotaExceededException">
		/// If file creation violates name space
		/// quota restriction
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">create not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidPathException">
		/// Path <code>src</code> is invalid
		/// <p>
		/// <em>Note that create with
		/// <see cref="Org.Apache.Hadoop.FS.CreateFlag.Overwrite"/>
		/// is idempotent.</em>
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AlreadyBeingCreatedException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract HdfsFileStatus Create(string src, FsPermission masked, string clientName
			, EnumSetWritable<CreateFlag> flag, bool createParent, short replication, long blockSize
			, CryptoProtocolVersion[] supportedVersions);

		/// <summary>Append to the end of the file.</summary>
		/// <param name="src">path of the file being created.</param>
		/// <param name="clientName">name of the current client.</param>
		/// <param name="flag">indicates whether the data is appended to a new block.</param>
		/// <returns>
		/// wrapper with information about the last partial block and file
		/// status if any
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">
		/// if permission to append file is
		/// denied by the system. As usually on the client side the exception will
		/// be wrapped into
		/// <see cref="Org.Apache.Hadoop.Ipc.RemoteException"/>
		/// .
		/// Allows appending to an existing file if the server is
		/// configured with the parameter dfs.support.append set to true, otherwise
		/// throws an IOException.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If permission to append to file is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="DSQuotaExceededException">
		/// If append violates disk space quota
		/// restriction
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">append not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred.
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="System.NotSupportedException">if append is not supported</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract LastBlockWithStatus Append(string src, string clientName, EnumSetWritable
			<CreateFlag> flag);

		/// <summary>Set replication for an existing file.</summary>
		/// <remarks>
		/// Set replication for an existing file.
		/// <p>
		/// The NameNode sets replication to the new value and returns.
		/// The actual block replication is not expected to be performed during
		/// this method call. The blocks will be populated or removed in the
		/// background as the result of the routine block maintenance procedures.
		/// </remarks>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="DSQuotaExceededException">
		/// If replication violates disk space
		/// quota restriction
		/// </exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract bool SetReplication(string src, short replication);

		/// <summary>Get all the available block storage policies.</summary>
		/// <returns>All the in-use block storage policies currently.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract BlockStoragePolicy[] GetStoragePolicies();

		/// <summary>Set the storage policy for a file/directory</summary>
		/// <param name="src">Path of an existing file/directory.</param>
		/// <param name="policyName">The name of the storage policy</param>
		/// <exception cref="SnapshotAccessControlException">If access is denied</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file/dir <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="QuotaExceededException">If changes violate the quota restriction
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void SetStoragePolicy(string src, string policyName);

		/// <summary>Set permissions for an existing file/directory.</summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract void SetPermission(string src, FsPermission permission);

		/// <summary>Set Owner of a path (i.e.</summary>
		/// <remarks>
		/// Set Owner of a path (i.e. a file or a directory).
		/// The parameters username and groupname cannot both be null.
		/// </remarks>
		/// <param name="src">file path</param>
		/// <param name="username">If it is null, the original username remains unchanged.</param>
		/// <param name="groupname">If it is null, the original groupname remains unchanged.</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract void SetOwner(string src, string username, string groupname);

		/// <summary>The client can give up on a block by calling abandonBlock().</summary>
		/// <remarks>
		/// The client can give up on a block by calling abandonBlock().
		/// The client can then either obtain a new block, or complete or abandon the
		/// file.
		/// Any partial writes to the block will be discarded.
		/// </remarks>
		/// <param name="b">Block to abandon</param>
		/// <param name="fileId">
		/// The id of the file where the block resides.  Older clients
		/// will pass GRANDFATHER_INODE_ID here.
		/// </param>
		/// <param name="src">The path of the file where the block resides.</param>
		/// <param name="holder">Lease holder.</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract void AbandonBlock(ExtendedBlock b, long fileId, string src, string
			 holder);

		/// <summary>
		/// A client that wants to write an additional block to the
		/// indicated filename (which must currently be open for writing)
		/// should call addBlock().
		/// </summary>
		/// <remarks>
		/// A client that wants to write an additional block to the
		/// indicated filename (which must currently be open for writing)
		/// should call addBlock().
		/// addBlock() allocates a new block and datanodes the block data
		/// should be replicated to.
		/// addBlock() also commits the previous block by reporting
		/// to the name-node the actual generation stamp and the length
		/// of the block that the client has transmitted to data-nodes.
		/// </remarks>
		/// <param name="src">the file being created</param>
		/// <param name="clientName">the name of the client that adds the block</param>
		/// <param name="previous">previous block</param>
		/// <param name="excludeNodes">
		/// a list of nodes that should not be
		/// allocated for the current block
		/// </param>
		/// <param name="fileId">the id uniquely identifying a file</param>
		/// <param name="favoredNodes">
		/// the list of nodes where the client wants the blocks.
		/// Nodes are identified by either host name or address.
		/// </param>
		/// <returns>LocatedBlock allocated block information.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NotReplicatedYetException
		/// 	">
		/// previous blocks of the file are not
		/// replicated yet. Blocks cannot be added until replication
		/// completes.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">create not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract LocatedBlock AddBlock(string src, string clientName, ExtendedBlock
			 previous, DatanodeInfo[] excludeNodes, long fileId, string[] favoredNodes);

		/// <summary>Get a datanode for an existing pipeline.</summary>
		/// <param name="src">the file being written</param>
		/// <param name="fileId">the ID of the file being written</param>
		/// <param name="blk">the block being written</param>
		/// <param name="existings">the existing nodes in the pipeline</param>
		/// <param name="excludes">the excluded nodes</param>
		/// <param name="numAdditionalNodes">number of additional datanodes</param>
		/// <param name="clientName">the name of the client</param>
		/// <returns>the located block.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">create not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract LocatedBlock GetAdditionalDatanode(string src, long fileId, ExtendedBlock
			 blk, DatanodeInfo[] existings, string[] existingStorageIDs, DatanodeInfo[] excludes
			, int numAdditionalNodes, string clientName);

		/// <summary>
		/// The client is done writing data to the given filename, and would
		/// like to complete it.
		/// </summary>
		/// <remarks>
		/// The client is done writing data to the given filename, and would
		/// like to complete it.
		/// The function returns whether the file has been closed successfully.
		/// If the function returns false, the caller should try again.
		/// close() also commits the last block of file by reporting
		/// to the name-node the actual generation stamp and the length
		/// of the block that the client has transmitted to data-nodes.
		/// A call to complete() will not return true until all the file's
		/// blocks have been replicated the minimum number of times.  Thus,
		/// DataNode failures may cause a client to call complete() several
		/// times before succeeding.
		/// </remarks>
		/// <param name="src">the file being created</param>
		/// <param name="clientName">the name of the client that adds the block</param>
		/// <param name="last">the last block info</param>
		/// <param name="fileId">the id uniquely identifying a file</param>
		/// <returns>true if all file blocks are minimally replicated or false otherwise</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">create not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract bool Complete(string src, string clientName, ExtendedBlock last, 
			long fileId);

		/// <summary>
		/// The client wants to report corrupted blocks (blocks with specified
		/// locations on datanodes).
		/// </summary>
		/// <param name="blocks">Array of located blocks to report</param>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void ReportBadBlocks(LocatedBlock[] blocks);

		///////////////////////////////////////
		// Namespace management
		///////////////////////////////////////
		/// <summary>Rename an item in the file system namespace.</summary>
		/// <param name="src">existing file or directory name.</param>
		/// <param name="dst">new name.</param>
		/// <returns>
		/// true if successful, or false if the old name does not exist
		/// or if the new name already belongs to the namespace.
		/// </returns>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract bool Rename(string src, string dst);

		/// <summary>Moves blocks from srcs to trg and delete srcs</summary>
		/// <param name="trg">existing file</param>
		/// <param name="srcs">- list of existing files (same block size, same replication)</param>
		/// <exception cref="System.IO.IOException">if some arguments are invalid</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">
		/// if <code>trg</code> or <code>srcs</code>
		/// contains a symlink
		/// </exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract void Concat(string trg, string[] srcs);

		/// <summary>Rename src to dst.</summary>
		/// <remarks>
		/// Rename src to dst.
		/// <ul>
		/// <li>Fails if src is a file and dst is a directory.
		/// <li>Fails if src is a directory and dst is a file.
		/// <li>Fails if the parent of dst does not exist or is a file.
		/// </ul>
		/// <p>
		/// Without OVERWRITE option, rename fails if the dst already exists.
		/// With OVERWRITE option, rename overwrites the dst, if it is a file
		/// or an empty directory. Rename fails if dst is a non-empty directory.
		/// <p>
		/// This implementation of rename is atomic.
		/// <p>
		/// </remarks>
		/// <param name="src">existing file or directory name.</param>
		/// <param name="dst">new name.</param>
		/// <param name="options">Rename options</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="DSQuotaExceededException">
		/// If rename violates disk space
		/// quota restriction
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException">
		/// If <code>dst</code> already exists and
		/// <code>options</options> has
		/// <see cref="Org.Apache.Hadoop.FS.Options.Rename.Overwrite"/>
		/// option
		/// false.
		/// </exception>
		/// <exception cref="System.IO.FileNotFoundException">If <code>src</code> does not exist
		/// 	</exception>
		/// <exception cref="NSQuotaExceededException">
		/// If rename violates namespace
		/// quota restriction
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException">
		/// If parent of <code>dst</code>
		/// is not a directory
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">rename not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">
		/// If <code>src</code> or
		/// <code>dst</code> contains a symlink
		/// </exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract void Rename2(string src, string dst, params Options.Rename[] options
			);

		/// <summary>Truncate file src to new size.</summary>
		/// <remarks>
		/// Truncate file src to new size.
		/// <ul>
		/// <li>Fails if src is a directory.
		/// <li>Fails if src does not exist.
		/// <li>Fails if src is not closed.
		/// <li>Fails if new size is greater than current size.
		/// </ul>
		/// <p>
		/// This implementation of truncate is purely a namespace operation if truncate
		/// occurs at a block boundary. Requires DataNode block recovery otherwise.
		/// <p>
		/// </remarks>
		/// <param name="src">existing file</param>
		/// <param name="newLength">the target size</param>
		/// <returns>
		/// true if client does not need to wait for block recovery,
		/// false if client needs to wait for block recovery.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">truncate not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract bool Truncate(string src, long newLength, string clientName);

		/// <summary>Delete the given file or directory from the file system.</summary>
		/// <remarks>
		/// Delete the given file or directory from the file system.
		/// <p>
		/// same as delete but provides a way to avoid accidentally
		/// deleting non empty directories programmatically.
		/// </remarks>
		/// <param name="src">existing name</param>
		/// <param name="recursive">
		/// if true deletes a non empty directory recursively,
		/// else throws an exception.
		/// </param>
		/// <returns>
		/// true only if the existing file or directory was actually removed
		/// from the file system.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">create not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract bool Delete(string src, bool recursive);

		/// <summary>
		/// Create a directory (or hierarchy of directories) with the given
		/// name and permission.
		/// </summary>
		/// <param name="src">The path of the directory being created</param>
		/// <param name="masked">The masked permission of the directory being created</param>
		/// <param name="createParent">create missing parent directory if true</param>
		/// <returns>True if the operation success.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException">If <code>src</code> already exists
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">
		/// If parent of <code>src</code> does not exist
		/// and <code>createParent</code> is false
		/// </exception>
		/// <exception cref="NSQuotaExceededException">If file creation violates quota restriction
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException">
		/// If parent of <code>src</code>
		/// is not a directory
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException">create not allowed in safemode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred.
		/// RunTimeExceptions:
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidPathException">If <code>src</code> is invalid
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract bool Mkdirs(string src, FsPermission masked, bool createParent);

		/// <summary>Get a partial listing of the indicated directory</summary>
		/// <param name="src">the directory name</param>
		/// <param name="startAfter">the name to start listing after encoded in java UTF8</param>
		/// <param name="needLocation">if the FileStatus should contain block locations</param>
		/// <returns>a partial listing starting after startAfter</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">If <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract DirectoryListing GetListing(string src, byte[] startAfter, bool needLocation
			);

		/// <summary>Get listing of all the snapshottable directories</summary>
		/// <returns>Information about all the current snapshottable directory</returns>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract SnapshottableDirectoryStatus[] GetSnapshottableDirListing();

		///////////////////////////////////////
		// System issues and management
		///////////////////////////////////////
		/// <summary>
		/// Client programs can cause stateful changes in the NameNode
		/// that affect other clients.
		/// </summary>
		/// <remarks>
		/// Client programs can cause stateful changes in the NameNode
		/// that affect other clients.  A client may obtain a file and
		/// neither abandon nor complete it.  A client might hold a series
		/// of locks that prevent other clients from proceeding.
		/// Clearly, it would be bad if a client held a bunch of locks
		/// that it never gave up.  This can happen easily if the client
		/// dies unexpectedly.
		/// <p>
		/// So, the NameNode will revoke the locks and live file-creates
		/// for clients that it thinks have died.  A client tells the
		/// NameNode that it is still alive by periodically calling
		/// renewLease().  If a certain amount of time passes since
		/// the last call to renewLease(), the NameNode assumes the
		/// client has died.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract void RenewLease(string clientName);

		/// <summary>Start lease recovery.</summary>
		/// <remarks>
		/// Start lease recovery.
		/// Lightweight NameNode operation to trigger lease recovery
		/// </remarks>
		/// <param name="src">path of the file to start lease recovery</param>
		/// <param name="clientName">name of the current client</param>
		/// <returns>true if the file is already closed</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract bool RecoverLease(string src, string clientName);

		public const int GetStatsCapacityIdx = 0;

		public const int GetStatsUsedIdx = 1;

		public const int GetStatsRemainingIdx = 2;

		public const int GetStatsUnderReplicatedIdx = 3;

		public const int GetStatsCorruptBlocksIdx = 4;

		public const int GetStatsMissingBlocksIdx = 5;

		public const int GetStatsMissingReplOneBlocksIdx = 6;

		/// <summary>Get a set of statistics about the filesystem.</summary>
		/// <remarks>
		/// Get a set of statistics about the filesystem.
		/// Right now, only seven values are returned.
		/// <ul>
		/// <li> [0] contains the total storage capacity of the system, in bytes.</li>
		/// <li> [1] contains the total used space of the system, in bytes.</li>
		/// <li> [2] contains the available storage of the system, in bytes.</li>
		/// <li> [3] contains number of under replicated blocks in the system.</li>
		/// <li> [4] contains number of blocks with a corrupt replica. </li>
		/// <li> [5] contains number of blocks without any good replicas left. </li>
		/// <li> [6] contains number of blocks which have replication factor
		/// 1 and have lost the only replica. </li>
		/// </ul>
		/// Use public constants like
		/// <see cref="GetStatsCapacityIdx"/>
		/// in place of
		/// actual numbers to index into the array.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract long[] GetStats();

		/// <summary>Get a report on the system's current datanodes.</summary>
		/// <remarks>
		/// Get a report on the system's current datanodes.
		/// One DatanodeInfo object is returned for each DataNode.
		/// Return live datanodes if type is LIVE; dead datanodes if type is DEAD;
		/// otherwise all datanodes if type is ALL.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract DatanodeInfo[] GetDatanodeReport(HdfsConstants.DatanodeReportType
			 type);

		/// <summary>Get a report on the current datanode storages.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract DatanodeStorageReport[] GetDatanodeStorageReport(HdfsConstants.DatanodeReportType
			 type);

		/// <summary>Get the block size for the given file.</summary>
		/// <param name="filename">The name of the file</param>
		/// <returns>The number of bytes in each block</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if the path contains a symlink.
		/// 	</exception>
		[Idempotent]
		public abstract long GetPreferredBlockSize(string filename);

		/// <summary>Enter, leave or get safe mode.</summary>
		/// <remarks>
		/// Enter, leave or get safe mode.
		/// <p>
		/// Safe mode is a name node state when it
		/// <ol><li>does not accept changes to name space (read-only), and</li>
		/// <li>does not replicate or delete blocks.</li></ol>
		/// <p>
		/// Safe mode is entered automatically at name node startup.
		/// Safe mode can also be entered manually using
		/// <see cref="SetSafeMode(SafeModeAction, bool)">setSafeMode(SafeModeAction.SAFEMODE_ENTER,false)
		/// 	</see>
		/// .
		/// <p>
		/// At startup the name node accepts data node reports collecting
		/// information about block locations.
		/// In order to leave safe mode it needs to collect a configurable
		/// percentage called threshold of blocks, which satisfy the minimal
		/// replication condition.
		/// The minimal replication condition is that each block must have at least
		/// <tt>dfs.namenode.replication.min</tt> replicas.
		/// When the threshold is reached the name node extends safe mode
		/// for a configurable amount of time
		/// to let the remaining data nodes to check in before it
		/// will start replicating missing blocks.
		/// Then the name node leaves safe mode.
		/// <p>
		/// If safe mode is turned on manually using
		/// <see cref="SetSafeMode(SafeModeAction, bool)">setSafeMode(SafeModeAction.SAFEMODE_ENTER,false)
		/// 	</see>
		/// then the name node stays in safe mode until it is manually turned off
		/// using
		/// <see cref="SetSafeMode(SafeModeAction, bool)">setSafeMode(SafeModeAction.SAFEMODE_LEAVE,false)
		/// 	</see>
		/// .
		/// Current state of the name node can be verified using
		/// <see cref="SetSafeMode(SafeModeAction, bool)">setSafeMode(SafeModeAction.SAFEMODE_GET,false)
		/// 	</see>
		/// <h4>Configuration parameters:</h4>
		/// <tt>dfs.safemode.threshold.pct</tt> is the threshold parameter.<br />
		/// <tt>dfs.safemode.extension</tt> is the safe mode extension parameter.<br />
		/// <tt>dfs.namenode.replication.min</tt> is the minimal replication parameter.
		/// <h4>Special cases:</h4>
		/// The name node does not enter safe mode at startup if the threshold is
		/// set to 0 or if the name space is empty.<br />
		/// If the threshold is set to 1 then all blocks need to have at least
		/// minimal replication.<br />
		/// If the threshold value is greater than 1 then the name node will not be
		/// able to turn off safe mode automatically.<br />
		/// Safe mode can always be turned off manually.
		/// </remarks>
		/// <param name="action">
		/// <ul> <li>0 leave safe mode;</li>
		/// <li>1 enter safe mode;</li>
		/// <li>2 get safe mode state.</li></ul>
		/// </param>
		/// <param name="isChecked">If true then action will be done only in ActiveNN.</param>
		/// <returns>
		/// <ul><li>0 if the safe mode is OFF or</li>
		/// <li>1 if the safe mode is ON.</li></ul>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract bool SetSafeMode(HdfsConstants.SafeModeAction action, bool isChecked
			);

		/// <summary>Save namespace image.</summary>
		/// <remarks>
		/// Save namespace image.
		/// <p>
		/// Saves current namespace into storage directories and reset edits log.
		/// Requires superuser privilege and safe mode.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the superuser privilege is violated.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if image creation failed.</exception>
		[AtMostOnce]
		public abstract void SaveNamespace();

		/// <summary>Roll the edit log.</summary>
		/// <remarks>
		/// Roll the edit log.
		/// Requires superuser privileges.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the superuser privilege is violated
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if log roll fails</exception>
		/// <returns>the txid of the new segment</returns>
		[Idempotent]
		public abstract long RollEdits();

		/// <summary>Enable/Disable restore failed storage.</summary>
		/// <remarks>
		/// Enable/Disable restore failed storage.
		/// <p>
		/// sets flag to enable restore of failed storage replicas
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the superuser privilege is violated.
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract bool RestoreFailedStorage(string arg);

		/// <summary>Tells the namenode to reread the hosts and exclude files.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RefreshNodes();

		/// <summary>Finalize previous upgrade.</summary>
		/// <remarks>
		/// Finalize previous upgrade.
		/// Remove file system state saved during the upgrade.
		/// The upgrade will become irreversible.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void FinalizeUpgrade();

		/// <summary>Rolling upgrade operations.</summary>
		/// <param name="action">either query, prepare or finalize.</param>
		/// <returns>
		/// rolling upgrade information. On query, if no upgrade is in
		/// progress, returns null.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract RollingUpgradeInfo RollingUpgrade(HdfsConstants.RollingUpgradeAction
			 action);

		/// <returns>
		/// CorruptFileBlocks, containing a list of corrupt files (with
		/// duplicates if there is more than one corrupt block in a file)
		/// and a cookie
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// Each call returns a subset of the corrupt files in the system. To obtain
		/// all corrupt files, call this method repeatedly and each time pass in the
		/// cookie returned from the previous call.
		/// </exception>
		[Idempotent]
		public abstract CorruptFileBlocks ListCorruptFileBlocks(string path, string cookie
			);

		/// <summary>Dumps namenode data structures into specified file.</summary>
		/// <remarks>
		/// Dumps namenode data structures into specified file. If the file
		/// already exists, then append.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void MetaSave(string filename);

		/// <summary>
		/// Tell all datanodes to use a new, non-persistent bandwidth value for
		/// dfs.balance.bandwidthPerSec.
		/// </summary>
		/// <param name="bandwidth">Blanacer bandwidth in bytes per second for this datanode.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void SetBalancerBandwidth(long bandwidth);

		/// <summary>Get the file info for a specific file or directory.</summary>
		/// <param name="src">The string representation of the path to the file</param>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if the path contains a symlink.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract HdfsFileStatus GetFileInfo(string src);

		/// <summary>Get the close status of a file</summary>
		/// <param name="src">The string representation of the path to the file</param>
		/// <returns>return true if file is closed</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if the path contains a symlink.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract bool IsFileClosed(string src);

		/// <summary>Get the file info for a specific file or directory.</summary>
		/// <remarks>
		/// Get the file info for a specific file or directory. If the path
		/// refers to a symlink then the FileStatus of the symlink is returned.
		/// </remarks>
		/// <param name="src">The string representation of the path to the file</param>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>src</code> contains a symlink
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract HdfsFileStatus GetFileLinkInfo(string src);

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.FS.ContentSummary"/>
		/// rooted at the specified directory.
		/// </summary>
		/// <param name="path">The string representation of the path</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>path</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>path</code> contains a symlink.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract ContentSummary GetContentSummary(string path);

		/// <summary>Set the quota for a directory.</summary>
		/// <param name="path">The string representation of the path to the directory</param>
		/// <param name="namespaceQuota">
		/// Limit on the number of names in the tree rooted
		/// at the directory
		/// </param>
		/// <param name="storagespaceQuota">
		/// Limit on storage space occupied all the files under
		/// this directory.
		/// </param>
		/// <param name="type">
		/// StorageType that the space quota is intended to be set on.
		/// It may be null when called by traditional space/namespace quota.
		/// When type is is not null, the storagespaceQuota parameter is for
		/// type specified and namespaceQuota must be
		/// <see cref="HdfsConstants.QuotaDontSet"/>
		/// .
		/// <br /><br />
		/// The quota can have three types of values : (1) 0 or more will set
		/// the quota to that value, (2)
		/// <see cref="HdfsConstants.QuotaDontSet"/>
		/// implies
		/// the quota will not be changed, and (3)
		/// <see cref="HdfsConstants.QuotaReset"/>
		/// 
		/// implies the quota will be reset. Any other value is a runtime error.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>path</code> is not found
		/// 	</exception>
		/// <exception cref="QuotaExceededException">
		/// if the directory size
		/// is greater than the given quota
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if the <code>path</code> contains a symlink.
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract void SetQuota(string path, long namespaceQuota, long storagespaceQuota
			, StorageType type);

		/// <summary>Write all metadata for this file into persistent storage.</summary>
		/// <remarks>
		/// Write all metadata for this file into persistent storage.
		/// The file must be currently open for writing.
		/// </remarks>
		/// <param name="src">The string representation of the path</param>
		/// <param name="inodeId">
		/// The inode ID, or GRANDFATHER_INODE_ID if the client is
		/// too old to support fsync with inode IDs.
		/// </param>
		/// <param name="client">The string representation of the client</param>
		/// <param name="lastBlockLength">
		/// The length of the last block (under construction)
		/// to be reported to NameNode
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>src</code> contains a symlink.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		[Idempotent]
		public abstract void Fsync(string src, long inodeId, string client, long lastBlockLength
			);

		/// <summary>Sets the modification and access time of the file to the specified time.
		/// 	</summary>
		/// <param name="src">The string representation of the path</param>
		/// <param name="mtime">
		/// The number of milliseconds since Jan 1, 1970.
		/// Setting mtime to -1 means that modification time should not be set
		/// by this call.
		/// </param>
		/// <param name="atime">
		/// The number of milliseconds since Jan 1, 1970.
		/// Setting atime to -1 means that access time should not be set
		/// by this call.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">file <code>src</code> is not found
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>src</code> contains a symlink.
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[Idempotent]
		public abstract void SetTimes(string src, long mtime, long atime);

		/// <summary>Create symlink to a file or directory.</summary>
		/// <param name="target">
		/// The path of the destination that the
		/// link points to.
		/// </param>
		/// <param name="link">The path of the link being created.</param>
		/// <param name="dirPerm">permissions to use when creating parent directories</param>
		/// <param name="createParent">
		/// - if true then missing parent dirs are created
		/// if false then parent must exist
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException">If file <code>link</code> already exists
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">
		/// If parent of <code>link</code> does not exist
		/// and <code>createParent</code> is false
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException">
		/// If parent of <code>link</code> is not a
		/// directory.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if <code>link</target> contains a symlink.
		/// 	</exception>
		/// <exception cref="SnapshotAccessControlException">if path is in RO snapshot</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		[AtMostOnce]
		public abstract void CreateSymlink(string target, string link, FsPermission dirPerm
			, bool createParent);

		/// <summary>Return the target of the given symlink.</summary>
		/// <remarks>
		/// Return the target of the given symlink. If there is an intermediate
		/// symlink in the path (ie a symlink leading up to the final path component)
		/// then the given path is returned with this symlink resolved.
		/// </remarks>
		/// <param name="path">The path with a link that needs resolution.</param>
		/// <returns>The path after resolving the first symbolic link in the path.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">permission denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">If <code>path</code> does not exist
		/// 	</exception>
		/// <exception cref="System.IO.IOException">
		/// If the given path does not refer to a symlink
		/// or an I/O error occurred
		/// </exception>
		[Idempotent]
		public abstract string GetLinkTarget(string path);

		/// <summary>
		/// Get a new generation stamp together with an access token for
		/// a block under construction
		/// This method is called only when a client needs to recover a failed
		/// pipeline or set up a pipeline for appending to a block.
		/// </summary>
		/// <param name="block">a block</param>
		/// <param name="clientName">the name of the client</param>
		/// <returns>a located block with a new generation stamp and an access token</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		[Idempotent]
		public abstract LocatedBlock UpdateBlockForPipeline(ExtendedBlock block, string clientName
			);

		/// <summary>Update a pipeline for a block under construction</summary>
		/// <param name="clientName">the name of the client</param>
		/// <param name="oldBlock">the old block</param>
		/// <param name="newBlock">the new block containing new generation stamp and length</param>
		/// <param name="newNodes">datanodes in the pipeline</param>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		[AtMostOnce]
		public abstract void UpdatePipeline(string clientName, ExtendedBlock oldBlock, ExtendedBlock
			 newBlock, DatanodeID[] newNodes, string[] newStorageIDs);

		/// <summary>Get a valid Delegation Token.</summary>
		/// <param name="renewer">the designated renewer for the token</param>
		/// <returns>Token<DelegationTokenIdentifier></returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>
			 GetDelegationToken(Text renewer);

		/// <summary>Renew an existing delegation token.</summary>
		/// <param name="token">delegation token obtained earlier</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token);

		/// <summary>Cancel an existing delegation token.</summary>
		/// <param name="token">delegation token</param>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token);

		/// <returns>
		/// encryption key so a client can encrypt data sent via the
		/// DataTransferProtocol to/from DataNodes.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract DataEncryptionKey GetDataEncryptionKey();

		/// <summary>Create a snapshot</summary>
		/// <param name="snapshotRoot">the path that is being snapshotted</param>
		/// <param name="snapshotName">name of the snapshot created</param>
		/// <returns>the snapshot path.</returns>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract string CreateSnapshot(string snapshotRoot, string snapshotName);

		/// <summary>Delete a specific snapshot of a snapshottable directory</summary>
		/// <param name="snapshotRoot">The snapshottable directory</param>
		/// <param name="snapshotName">Name of the snapshot for the snapshottable directory</param>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract void DeleteSnapshot(string snapshotRoot, string snapshotName);

		/// <summary>Rename a snapshot</summary>
		/// <param name="snapshotRoot">the directory path where the snapshot was taken</param>
		/// <param name="snapshotOldName">old name of the snapshot</param>
		/// <param name="snapshotNewName">new name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract void RenameSnapshot(string snapshotRoot, string snapshotOldName, 
			string snapshotNewName);

		/// <summary>Allow snapshot on a directory.</summary>
		/// <param name="snapshotRoot">the directory to be snapped</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		[Idempotent]
		public abstract void AllowSnapshot(string snapshotRoot);

		/// <summary>Disallow snapshot on a directory.</summary>
		/// <param name="snapshotRoot">the directory to disallow snapshot</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		[Idempotent]
		public abstract void DisallowSnapshot(string snapshotRoot);

		/// <summary>
		/// Get the difference between two snapshots, or between a snapshot and the
		/// current tree of a directory.
		/// </summary>
		/// <param name="snapshotRoot">full path of the directory where snapshots are taken</param>
		/// <param name="fromSnapshot">
		/// snapshot name of the from point. Null indicates the current
		/// tree
		/// </param>
		/// <param name="toSnapshot">
		/// snapshot name of the to point. Null indicates the current
		/// tree.
		/// </param>
		/// <returns>
		/// The difference report represented as a
		/// <see cref="SnapshotDiffReport"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		[Idempotent]
		public abstract SnapshotDiffReport GetSnapshotDiffReport(string snapshotRoot, string
			 fromSnapshot, string toSnapshot);

		/// <summary>Add a CacheDirective to the CacheManager.</summary>
		/// <param name="directive">A CacheDirectiveInfo to be added</param>
		/// <param name="flags">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.CacheFlag"/>
		/// s to use for this operation.
		/// </param>
		/// <returns>A CacheDirectiveInfo associated with the added directive</returns>
		/// <exception cref="System.IO.IOException">if the directive could not be added</exception>
		[AtMostOnce]
		public abstract long AddCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag
			> flags);

		/// <summary>Modify a CacheDirective in the CacheManager.</summary>
		/// <param name="flags">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.CacheFlag"/>
		/// s to use for this operation.
		/// </param>
		/// <exception cref="System.IO.IOException">if the directive could not be modified</exception>
		[AtMostOnce]
		public abstract void ModifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag
			> flags);

		/// <summary>Remove a CacheDirectiveInfo from the CacheManager.</summary>
		/// <param name="id">of a CacheDirectiveInfo</param>
		/// <exception cref="System.IO.IOException">if the cache directive could not be removed
		/// 	</exception>
		[AtMostOnce]
		public abstract void RemoveCacheDirective(long id);

		/// <summary>List the set of cached paths of a cache pool.</summary>
		/// <remarks>
		/// List the set of cached paths of a cache pool. Incrementally fetches results
		/// from the server.
		/// </remarks>
		/// <param name="prevId">
		/// The last listed entry ID, or -1 if this is the first call to
		/// listCacheDirectives.
		/// </param>
		/// <param name="filter">
		/// Parameters to use to filter the list results,
		/// or null to display all directives visible to us.
		/// </param>
		/// <returns>A batch of CacheDirectiveEntry objects.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> ListCacheDirectives
			(long prevId, CacheDirectiveInfo filter);

		/// <summary>Add a new cache pool.</summary>
		/// <param name="info">Description of the new cache pool</param>
		/// <exception cref="System.IO.IOException">If the request could not be completed.</exception>
		[AtMostOnce]
		public abstract void AddCachePool(CachePoolInfo info);

		/// <summary>Modify an existing cache pool.</summary>
		/// <param name="req">The request to modify a cache pool.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// If the request could not be completed.
		/// </exception>
		[AtMostOnce]
		public abstract void ModifyCachePool(CachePoolInfo req);

		/// <summary>Remove a cache pool.</summary>
		/// <param name="pool">name of the cache pool to remove.</param>
		/// <exception cref="System.IO.IOException">
		/// if the cache pool did not exist, or could not be
		/// removed.
		/// </exception>
		[AtMostOnce]
		public abstract void RemoveCachePool(string pool);

		/// <summary>List the set of cache pools.</summary>
		/// <remarks>List the set of cache pools. Incrementally fetches results from the server.
		/// 	</remarks>
		/// <param name="prevPool">
		/// name of the last pool listed, or the empty string if this is
		/// the first invocation of listCachePools
		/// </param>
		/// <returns>A batch of CachePoolEntry objects.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract BatchedRemoteIterator.BatchedEntries<CachePoolEntry> ListCachePools
			(string prevPool);

		/// <summary>Modifies ACL entries of files and directories.</summary>
		/// <remarks>
		/// Modifies ACL entries of files and directories.  This method can add new ACL
		/// entries or modify the permissions on existing ACL entries.  All existing
		/// ACL entries that are not specified in this call are retained without
		/// changes.  (Modifications are merged into the current ACL.)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void ModifyAclEntries(string src, IList<AclEntry> aclSpec);

		/// <summary>Removes ACL entries from files and directories.</summary>
		/// <remarks>
		/// Removes ACL entries from files and directories.  Other ACL entries are
		/// retained.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RemoveAclEntries(string src, IList<AclEntry> aclSpec);

		/// <summary>Removes all default ACL entries from files and directories.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RemoveDefaultAcl(string src);

		/// <summary>Removes all but the base ACL entries of files and directories.</summary>
		/// <remarks>
		/// Removes all but the base ACL entries of files and directories.  The entries
		/// for user, group, and others are retained for compatibility with permission
		/// bits.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RemoveAcl(string src);

		/// <summary>
		/// Fully replaces ACL of files and directories, discarding all existing
		/// entries.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void SetAcl(string src, IList<AclEntry> aclSpec);

		/// <summary>Gets the ACLs of files and directories.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract AclStatus GetAclStatus(string src);

		/// <summary>Create an encryption zone</summary>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract void CreateEncryptionZone(string src, string keyName);

		/// <summary>Get the encryption zone for a path.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract EncryptionZone GetEZForPath(string src);

		/// <summary>
		/// Used to implement cursor-based batched listing of
		/// <EncryptionZone/>
		/// s.
		/// </summary>
		/// <param name="prevId">
		/// ID of the last item in the previous batch. If there is no
		/// previous batch, a negative value can be used.
		/// </param>
		/// <returns>Batch of encryption zones.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract BatchedRemoteIterator.BatchedEntries<EncryptionZone> ListEncryptionZones
			(long prevId);

		/// <summary>Set xattr of a file or directory.</summary>
		/// <remarks>
		/// Set xattr of a file or directory.
		/// The name must be prefixed with the namespace followed by ".". For example,
		/// "user.attr".
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="src">file or directory</param>
		/// <param name="xAttr"><code>XAttr</code> to set</param>
		/// <param name="flag">set flag</param>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract void SetXAttr(string src, XAttr xAttr, EnumSet<XAttrSetFlag> flag
			);

		/// <summary>Get xattrs of a file or directory.</summary>
		/// <remarks>
		/// Get xattrs of a file or directory. Values in xAttrs parameter are ignored.
		/// If xAttrs is null or empty, this is the same as getting all xattrs of the
		/// file or directory.  Only those xattrs for which the logged-in user has
		/// permissions to view are returned.
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="src">file or directory</param>
		/// <param name="xAttrs">xAttrs to get</param>
		/// <returns>List<XAttr> <code>XAttr</code> list</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract IList<XAttr> GetXAttrs(string src, IList<XAttr> xAttrs);

		/// <summary>List the xattrs names for a file or directory.</summary>
		/// <remarks>
		/// List the xattrs names for a file or directory.
		/// Only the xattr names for which the logged in user has the permissions to
		/// access will be returned.
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="src">file or directory</param>
		/// <returns>List<XAttr> <code>XAttr</code> list</returns>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract IList<XAttr> ListXAttrs(string src);

		/// <summary>Remove xattr of a file or directory.Value in xAttr parameter is ignored.
		/// 	</summary>
		/// <remarks>
		/// Remove xattr of a file or directory.Value in xAttr parameter is ignored.
		/// The name must be prefixed with the namespace followed by ".". For example,
		/// "user.attr".
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="src">file or directory</param>
		/// <param name="xAttr"><code>XAttr</code> to remove</param>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		public abstract void RemoveXAttr(string src, XAttr xAttr);

		/// <summary>Checks if the user can access a path.</summary>
		/// <remarks>
		/// Checks if the user can access a path.  The mode specifies which access
		/// checks to perform.  If the requested permissions are granted, then the
		/// method returns normally.  If access is denied, then the method throws an
		/// <see cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// .
		/// In general, applications should avoid using this method, due to the risk of
		/// time-of-check/time-of-use race conditions.  The permissions on a file may
		/// change immediately after the access call returns.
		/// </remarks>
		/// <param name="path">Path to check</param>
		/// <param name="mode">type of access to check</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">if the path does not exist</exception>
		/// <exception cref="System.IO.IOException">see specific implementation</exception>
		[Idempotent]
		public abstract void CheckAccess(string path, FsAction mode);

		/// <summary>
		/// Get the highest txid the NameNode knows has been written to the edit
		/// log, or -1 if the NameNode's edit log is not yet open for write.
		/// </summary>
		/// <remarks>
		/// Get the highest txid the NameNode knows has been written to the edit
		/// log, or -1 if the NameNode's edit log is not yet open for write. Used as
		/// the starting point for the inotify event stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract long GetCurrentEditLogTxid();

		/// <summary>
		/// Get an ordered list of batches of events corresponding to the edit log
		/// transactions for txids equal to or greater than txid.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract EventBatchList GetEditsFromTxid(long txid);
	}

	public static class ClientProtocolConstants
	{
	}
}

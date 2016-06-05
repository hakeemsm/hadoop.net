using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Both FSDirectory and FSNamesystem manage the state of the namespace.</summary>
	/// <remarks>
	/// Both FSDirectory and FSNamesystem manage the state of the namespace.
	/// FSDirectory is a pure in-memory data structure, all of whose operations
	/// happen entirely in memory. In contrast, FSNamesystem persists the operations
	/// to the disk.
	/// </remarks>
	/// <seealso cref="FSNamesystem"/>
	public class FSDirectory : IDisposable
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSDirectory
			));

		private static INodeDirectory CreateRoot(FSNamesystem namesystem)
		{
			INodeDirectory r = new INodeDirectory(INodeId.RootInodeId, INodeDirectory.RootName
				, namesystem.CreateFsOwnerPermissions(new FsPermission((short)0x1ed)), 0L);
			r.AddDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().NameSpaceQuota
				(DirectoryWithQuotaFeature.DefaultNamespaceQuota).StorageSpaceQuota(DirectoryWithQuotaFeature
				.DefaultStorageSpaceQuota).Build());
			r.AddSnapshottableFeature();
			r.SetSnapshotQuota(0);
			return r;
		}

		[VisibleForTesting]
		internal static bool CheckReservedFileNames = true;

		public const string DotReservedString = ".reserved";

		public const string DotReservedPathPrefix = Path.Separator + DotReservedString;

		public static readonly byte[] DotReserved = DFSUtil.String2Bytes(DotReservedString
			);

		private const string RawString = "raw";

		private static readonly byte[] Raw = DFSUtil.String2Bytes(RawString);

		public const string DotInodesString = ".inodes";

		public static readonly byte[] DotInodes = DFSUtil.String2Bytes(DotInodesString);

		internal INodeDirectory rootDir;

		private readonly FSNamesystem namesystem;

		private volatile bool skipQuotaCheck = false;

		private readonly int maxComponentLength;

		private readonly int maxDirItems;

		private readonly int lsLimit;

		private readonly int contentCountLimit;

		private readonly long contentSleepMicroSec;

		private readonly INodeMap inodeMap;

		private long yieldCount = 0;

		private readonly int inodeXAttrsLimit;

		private readonly ReentrantReadWriteLock dirLock;

		private readonly bool isPermissionEnabled;

		/// <summary>Support for ACLs is controlled by a configuration flag.</summary>
		/// <remarks>
		/// Support for ACLs is controlled by a configuration flag. If the
		/// configuration flag is false, then the NameNode will reject all
		/// ACL-related operations.
		/// </remarks>
		private readonly bool aclsEnabled;

		private readonly bool xattrsEnabled;

		private readonly int xattrMaxSize;

		private readonly long accessTimePrecision;

		private readonly bool storagePolicyEnabled;

		private readonly bool quotaByStorageTypeEnabled;

		private readonly string fsOwnerShortUserName;

		private readonly string supergroup;

		private readonly INodeId inodeId;

		private readonly FSEditLog editLog;

		private INodeAttributeProvider attributeProvider;

		//skip while consuming edits
		// max list limit
		// max content summary counts per run
		// Synchronized by dirLock
		// keep track of lock yield count.
		//inode xattrs max limit
		// lock to protect the directory and BlockMap
		// precision of access times.
		// whether setStoragePolicy is allowed.
		// whether quota by storage type is allowed
		public virtual void SetINodeAttributeProvider(INodeAttributeProvider provider)
		{
			attributeProvider = provider;
		}

		// utility methods to acquire and release read lock and write lock
		internal virtual void ReadLock()
		{
			this.dirLock.ReadLock().Lock();
		}

		internal virtual void ReadUnlock()
		{
			this.dirLock.ReadLock().Unlock();
		}

		internal virtual void WriteLock()
		{
			this.dirLock.WriteLock().Lock();
		}

		internal virtual void WriteUnlock()
		{
			this.dirLock.WriteLock().Unlock();
		}

		internal virtual bool HasWriteLock()
		{
			return this.dirLock.IsWriteLockedByCurrentThread();
		}

		internal virtual bool HasReadLock()
		{
			return this.dirLock.GetReadHoldCount() > 0 || HasWriteLock();
		}

		public virtual int GetReadHoldCount()
		{
			return this.dirLock.GetReadHoldCount();
		}

		public virtual int GetWriteHoldCount()
		{
			return this.dirLock.GetWriteHoldCount();
		}

		[VisibleForTesting]
		public readonly EncryptionZoneManager ezManager;

		/// <summary>
		/// Caches frequently used file names used in
		/// <see cref="INode"/>
		/// to reuse
		/// byte[] objects and reduce heap usage.
		/// </summary>
		private readonly NameCache<ByteArray> nameCache;

		/// <exception cref="System.IO.IOException"/>
		internal FSDirectory(FSNamesystem ns, Configuration conf)
		{
			this.dirLock = new ReentrantReadWriteLock(true);
			// fair
			this.inodeId = new INodeId();
			rootDir = CreateRoot(ns);
			inodeMap = INodeMap.NewInstance(rootDir);
			this.isPermissionEnabled = conf.GetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey
				, DFSConfigKeys.DfsPermissionsEnabledDefault);
			this.fsOwnerShortUserName = UserGroupInformation.GetCurrentUser().GetShortUserName
				();
			this.supergroup = conf.Get(DFSConfigKeys.DfsPermissionsSuperusergroupKey, DFSConfigKeys
				.DfsPermissionsSuperusergroupDefault);
			this.aclsEnabled = conf.GetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, DFSConfigKeys
				.DfsNamenodeAclsEnabledDefault);
			Log.Info("ACLs enabled? " + aclsEnabled);
			this.xattrsEnabled = conf.GetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, DFSConfigKeys
				.DfsNamenodeXattrsEnabledDefault);
			Log.Info("XAttrs enabled? " + xattrsEnabled);
			this.xattrMaxSize = conf.GetInt(DFSConfigKeys.DfsNamenodeMaxXattrSizeKey, DFSConfigKeys
				.DfsNamenodeMaxXattrSizeDefault);
			Preconditions.CheckArgument(xattrMaxSize >= 0, "Cannot set a negative value for the maximum size of an xattr (%s)."
				, DFSConfigKeys.DfsNamenodeMaxXattrSizeKey);
			string unlimited = xattrMaxSize == 0 ? " (unlimited)" : string.Empty;
			Log.Info("Maximum size of an xattr: " + xattrMaxSize + unlimited);
			this.accessTimePrecision = conf.GetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey
				, DFSConfigKeys.DfsNamenodeAccesstimePrecisionDefault);
			this.storagePolicyEnabled = conf.GetBoolean(DFSConfigKeys.DfsStoragePolicyEnabledKey
				, DFSConfigKeys.DfsStoragePolicyEnabledDefault);
			this.quotaByStorageTypeEnabled = conf.GetBoolean(DFSConfigKeys.DfsQuotaByStoragetypeEnabledKey
				, DFSConfigKeys.DfsQuotaByStoragetypeEnabledDefault);
			int configuredLimit = conf.GetInt(DFSConfigKeys.DfsListLimit, DFSConfigKeys.DfsListLimitDefault
				);
			this.lsLimit = configuredLimit > 0 ? configuredLimit : DFSConfigKeys.DfsListLimitDefault;
			this.contentCountLimit = conf.GetInt(DFSConfigKeys.DfsContentSummaryLimitKey, DFSConfigKeys
				.DfsContentSummaryLimitDefault);
			this.contentSleepMicroSec = conf.GetLong(DFSConfigKeys.DfsContentSummarySleepMicrosecKey
				, DFSConfigKeys.DfsContentSummarySleepMicrosecDefault);
			// filesystem limits
			this.maxComponentLength = conf.GetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey
				, DFSConfigKeys.DfsNamenodeMaxComponentLengthDefault);
			this.maxDirItems = conf.GetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, DFSConfigKeys
				.DfsNamenodeMaxDirectoryItemsDefault);
			this.inodeXAttrsLimit = conf.GetInt(DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeKey
				, DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeDefault);
			Preconditions.CheckArgument(this.inodeXAttrsLimit >= 0, "Cannot set a negative limit on the number of xattrs per inode (%s)."
				, DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeKey);
			// We need a maximum maximum because by default, PB limits message sizes
			// to 64MB. This means we can only store approximately 6.7 million entries
			// per directory, but let's use 6.4 million for some safety.
			int MaxDirItems = 64 * 100 * 1000;
			Preconditions.CheckArgument(maxDirItems > 0 && maxDirItems <= MaxDirItems, "Cannot set "
				 + DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey + " to a value less than 1 or greater than "
				 + MaxDirItems);
			int threshold = conf.GetInt(DFSConfigKeys.DfsNamenodeNameCacheThresholdKey, DFSConfigKeys
				.DfsNamenodeNameCacheThresholdDefault);
			NameNode.Log.Info("Caching file names occuring more than " + threshold + " times"
				);
			nameCache = new NameCache<ByteArray>(threshold);
			namesystem = ns;
			this.editLog = ns.GetEditLog();
			ezManager = new EncryptionZoneManager(this, conf);
		}

		internal virtual FSNamesystem GetFSNamesystem()
		{
			return namesystem;
		}

		private BlockManager GetBlockManager()
		{
			return GetFSNamesystem().GetBlockManager();
		}

		/// <returns>the root directory inode.</returns>
		public virtual INodeDirectory GetRoot()
		{
			return rootDir;
		}

		public virtual BlockStoragePolicySuite GetBlockStoragePolicySuite()
		{
			return GetBlockManager().GetStoragePolicySuite();
		}

		internal virtual bool IsPermissionEnabled()
		{
			return isPermissionEnabled;
		}

		internal virtual bool IsAclsEnabled()
		{
			return aclsEnabled;
		}

		internal virtual bool IsXattrsEnabled()
		{
			return xattrsEnabled;
		}

		internal virtual int GetXattrMaxSize()
		{
			return xattrMaxSize;
		}

		internal virtual bool IsStoragePolicyEnabled()
		{
			return storagePolicyEnabled;
		}

		internal virtual bool IsAccessTimeSupported()
		{
			return accessTimePrecision > 0;
		}

		internal virtual bool IsQuotaByStorageTypeEnabled()
		{
			return quotaByStorageTypeEnabled;
		}

		internal virtual int GetLsLimit()
		{
			return lsLimit;
		}

		internal virtual int GetContentCountLimit()
		{
			return contentCountLimit;
		}

		internal virtual long GetContentSleepMicroSec()
		{
			return contentSleepMicroSec;
		}

		internal virtual int GetInodeXAttrsLimit()
		{
			return inodeXAttrsLimit;
		}

		internal virtual FSEditLog GetEditLog()
		{
			return editLog;
		}

		/// <summary>Shutdown the filestore</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
		}

		internal virtual void MarkNameCacheInitialized()
		{
			WriteLock();
			try
			{
				nameCache.Initialized();
			}
			finally
			{
				WriteUnlock();
			}
		}

		internal virtual bool ShouldSkipQuotaChecks()
		{
			return skipQuotaCheck;
		}

		/// <summary>Enable quota verification</summary>
		internal virtual void EnableQuotaChecks()
		{
			skipQuotaCheck = false;
		}

		/// <summary>Disable quota verification</summary>
		internal virtual void DisableQuotaChecks()
		{
			skipQuotaCheck = true;
		}

		private static INodeFile NewINodeFile(long id, PermissionStatus permissions, long
			 mtime, long atime, short replication, long preferredBlockSize)
		{
			return NewINodeFile(id, permissions, mtime, atime, replication, preferredBlockSize
				, unchecked((byte)0));
		}

		private static INodeFile NewINodeFile(long id, PermissionStatus permissions, long
			 mtime, long atime, short replication, long preferredBlockSize, byte storagePolicyId
			)
		{
			return new INodeFile(id, null, permissions, mtime, atime, BlockInfoContiguous.EmptyArray
				, replication, preferredBlockSize, storagePolicyId);
		}

		/// <summary>Add the given filename to the fs.</summary>
		/// <returns>the new INodesInPath instance that contains the new INode</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		internal virtual INodesInPath AddFile(INodesInPath existing, string localName, PermissionStatus
			 permissions, short replication, long preferredBlockSize, string clientName, string
			 clientMachine)
		{
			long modTime = Time.Now();
			INodeFile newNode = NewINodeFile(AllocateNewInodeId(), permissions, modTime, modTime
				, replication, preferredBlockSize);
			newNode.SetLocalName(Sharpen.Runtime.GetBytesForString(localName, Charsets.Utf8));
			newNode.ToUnderConstruction(clientName, clientMachine);
			INodesInPath newiip;
			WriteLock();
			try
			{
				newiip = AddINode(existing, newNode);
			}
			finally
			{
				WriteUnlock();
			}
			if (newiip == null)
			{
				NameNode.stateChangeLog.Info("DIR* addFile: failed to add " + existing.GetPath() 
					+ "/" + localName);
				return null;
			}
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* addFile: " + localName + " is added");
			}
			return newiip;
		}

		internal virtual INodeFile AddFileForEditLog(long id, INodesInPath existing, byte
			[] localName, PermissionStatus permissions, IList<AclEntry> aclEntries, IList<XAttr
			> xAttrs, short replication, long modificationTime, long atime, long preferredBlockSize
			, bool underConstruction, string clientName, string clientMachine, byte storagePolicyId
			)
		{
			INodeFile newNode;
			System.Diagnostics.Debug.Assert(HasWriteLock());
			if (underConstruction)
			{
				newNode = NewINodeFile(id, permissions, modificationTime, modificationTime, replication
					, preferredBlockSize, storagePolicyId);
				newNode.ToUnderConstruction(clientName, clientMachine);
			}
			else
			{
				newNode = NewINodeFile(id, permissions, modificationTime, atime, replication, preferredBlockSize
					, storagePolicyId);
			}
			newNode.SetLocalName(localName);
			try
			{
				INodesInPath iip = AddINode(existing, newNode);
				if (iip != null)
				{
					if (aclEntries != null)
					{
						AclStorage.UpdateINodeAcl(newNode, aclEntries, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
							.CurrentStateId);
					}
					if (xAttrs != null)
					{
						XAttrStorage.UpdateINodeXAttrs(newNode, xAttrs, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
							.CurrentStateId);
					}
					return newNode;
				}
			}
			catch (IOException e)
			{
				if (NameNode.stateChangeLog.IsDebugEnabled())
				{
					NameNode.stateChangeLog.Debug("DIR* FSDirectory.unprotectedAddFile: exception when add "
						 + existing.GetPath() + " to the file system", e);
				}
			}
			return null;
		}

		/// <summary>Add a block to the file.</summary>
		/// <remarks>Add a block to the file. Returns a reference to the added block.</remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual BlockInfoContiguous AddBlock(string path, INodesInPath inodesInPath
			, Block block, DatanodeStorageInfo[] targets)
		{
			WriteLock();
			try
			{
				INodeFile fileINode = inodesInPath.GetLastINode().AsFile();
				Preconditions.CheckState(fileINode.IsUnderConstruction());
				// check quota limits and updated space consumed
				UpdateCount(inodesInPath, 0, fileINode.GetPreferredBlockSize(), fileINode.GetBlockReplication
					(), true);
				// associate new last block for the file
				BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction
					(block, fileINode.GetFileReplication(), HdfsServerConstants.BlockUCState.UnderConstruction
					, targets);
				GetBlockManager().AddBlockCollection(blockInfo, fileINode);
				fileINode.AddBlock(blockInfo);
				if (NameNode.stateChangeLog.IsDebugEnabled())
				{
					NameNode.stateChangeLog.Debug("DIR* FSDirectory.addBlock: " + path + " with " + block
						 + " block is added to the in-memory " + "file system");
				}
				return blockInfo;
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Remove a block from the file.</summary>
		/// <returns>Whether the block exists in the corresponding file</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RemoveBlock(string path, INodesInPath iip, INodeFile fileNode
			, Block block)
		{
			Preconditions.CheckArgument(fileNode.IsUnderConstruction());
			WriteLock();
			try
			{
				return UnprotectedRemoveBlock(path, iip, fileNode, block);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool UnprotectedRemoveBlock(string path, INodesInPath iip, INodeFile
			 fileNode, Block block)
		{
			// modify file-> block and blocksMap
			// fileNode should be under construction
			bool removed = fileNode.RemoveLastBlock(block);
			if (!removed)
			{
				return false;
			}
			GetBlockManager().RemoveBlockFromMap(block);
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* FSDirectory.removeBlock: " + path + " with " 
					+ block + " block is removed from the file system");
			}
			// update space consumed
			UpdateCount(iip, 0, -fileNode.GetPreferredBlockSize(), fileNode.GetBlockReplication
				(), true);
			return true;
		}

		/// <summary>This is a wrapper for resolvePath().</summary>
		/// <remarks>
		/// This is a wrapper for resolvePath(). If the path passed
		/// is prefixed with /.reserved/raw, then it checks to ensure that the caller
		/// has super user privileges.
		/// </remarks>
		/// <param name="pc">The permission checker used when resolving path.</param>
		/// <param name="path">The path to resolve.</param>
		/// <param name="pathComponents">path components corresponding to the path</param>
		/// <returns>
		/// if the path indicates an inode, return path after replacing up to
		/// <inodeid> with the corresponding path of the inode, else the path
		/// in
		/// <c>src</c>
		/// as is. If the path refers to a path in the "raw"
		/// directory, return the non-raw pathname.
		/// </returns>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual string ResolvePath(FSPermissionChecker pc, string path, byte[][]
			 pathComponents)
		{
			if (IsReservedRawName(path) && isPermissionEnabled)
			{
				pc.CheckSuperuserPrivilege();
			}
			return ResolvePath(path, pathComponents, this);
		}

		/// <returns>true if the path is a non-empty directory; otherwise, return false.</returns>
		internal virtual bool IsNonEmptyDirectory(INodesInPath inodesInPath)
		{
			ReadLock();
			try
			{
				INode inode = inodesInPath.GetLastINode();
				if (inode == null || !inode.IsDirectory())
				{
					//not found or not a directory
					return false;
				}
				int s = inodesInPath.GetPathSnapshotId();
				return !inode.AsDirectory().GetChildrenList(s).IsEmpty();
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Check whether the filepath could be created</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException">if path is in RO snapshot
		/// 	</exception>
		internal virtual bool IsValidToCreate(string src, INodesInPath iip)
		{
			string srcs = NormalizePath(src);
			return srcs.StartsWith("/") && !srcs.EndsWith("/") && iip.GetLastINode() == null;
		}

		/// <summary>Check whether the path specifies a directory</summary>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual bool IsDir(string src)
		{
			src = NormalizePath(src);
			ReadLock();
			try
			{
				INode node = GetINode(src, false);
				return node != null && node.IsDirectory();
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>
		/// Updates namespace, storagespace and typespaces consumed for all
		/// directories until the parent directory of file represented by path.
		/// </summary>
		/// <param name="iip">
		/// the INodesInPath instance containing all the INodes for
		/// updating quota usage
		/// </param>
		/// <param name="nsDelta">the delta change of namespace</param>
		/// <param name="ssDelta">the delta change of storage space consumed without replication
		/// 	</param>
		/// <param name="replication">the replication factor of the block consumption change</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">if the new count violates any quota limit
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">if path does not exist.</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		internal virtual void UpdateSpaceConsumed(INodesInPath iip, long nsDelta, long ssDelta
			, short replication)
		{
			WriteLock();
			try
			{
				if (iip.GetLastINode() == null)
				{
					throw new FileNotFoundException("Path not found: " + iip.GetPath());
				}
				UpdateCount(iip, nsDelta, ssDelta, replication, true);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Update the quota usage after deletion.</summary>
		/// <remarks>
		/// Update the quota usage after deletion. The quota update is only necessary
		/// when image/edits have been loaded and the file/dir to be deleted is not
		/// contained in snapshots.
		/// </remarks>
		internal virtual void UpdateCountForDelete(INode inode, INodesInPath iip)
		{
			if (GetFSNamesystem().IsImageLoaded() && !inode.IsInLatestSnapshot(iip.GetLatestSnapshotId
				()))
			{
				QuotaCounts counts = inode.ComputeQuotaUsage(GetBlockStoragePolicySuite());
				UnprotectedUpdateCount(iip, iip.Length() - 1, counts.Negation());
			}
		}

		/// <summary>Update usage count without replication factor change</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		internal virtual void UpdateCount(INodesInPath iip, long nsDelta, long ssDelta, short
			 replication, bool checkQuota)
		{
			INodeFile fileINode = iip.GetLastINode().AsFile();
			EnumCounters<StorageType> typeSpaceDeltas = GetStorageTypeDeltas(fileINode.GetStoragePolicyID
				(), ssDelta, replication, replication);
			UpdateCount(iip, iip.Length() - 1, new QuotaCounts.Builder().NameSpace(nsDelta).StorageSpace
				(ssDelta * replication).TypeSpaces(typeSpaceDeltas).Build(), checkQuota);
		}

		/// <summary>Update usage count with replication factor change due to setReplication</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		internal virtual void UpdateCount(INodesInPath iip, long nsDelta, long ssDelta, short
			 oldRep, short newRep, bool checkQuota)
		{
			INodeFile fileINode = iip.GetLastINode().AsFile();
			EnumCounters<StorageType> typeSpaceDeltas = GetStorageTypeDeltas(fileINode.GetStoragePolicyID
				(), ssDelta, oldRep, newRep);
			UpdateCount(iip, iip.Length() - 1, new QuotaCounts.Builder().NameSpace(nsDelta).StorageSpace
				(ssDelta * (newRep - oldRep)).TypeSpaces(typeSpaceDeltas).Build(), checkQuota);
		}

		/// <summary>update count of each inode with quota</summary>
		/// <param name="iip">inodes in a path</param>
		/// <param name="numOfINodes">the number of inodes to update starting from index 0</param>
		/// <param name="counts">the count of space/namespace/type usage to be update</param>
		/// <param name="checkQuota">if true then check if quota is exceeded</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">if the new count violates any quota limit
		/// 	</exception>
		internal virtual void UpdateCount(INodesInPath iip, int numOfINodes, QuotaCounts 
			counts, bool checkQuota)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			if (!namesystem.IsImageLoaded())
			{
				//still initializing. do not check or update quotas.
				return;
			}
			if (numOfINodes > iip.Length())
			{
				numOfINodes = iip.Length();
			}
			if (checkQuota && !skipQuotaCheck)
			{
				VerifyQuota(iip, numOfINodes, counts, null);
			}
			UnprotectedUpdateCount(iip, numOfINodes, counts);
		}

		/// <summary>update quota of each inode and check to see if quota is exceeded.</summary>
		/// <remarks>
		/// update quota of each inode and check to see if quota is exceeded.
		/// See
		/// <see cref="UpdateCount(INodesInPath, int, QuotaCounts, bool)"/>
		/// </remarks>
		internal virtual void UpdateCountNoQuotaCheck(INodesInPath inodesInPath, int numOfINodes
			, QuotaCounts counts)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			try
			{
				UpdateCount(inodesInPath, numOfINodes, counts, false);
			}
			catch (QuotaExceededException e)
			{
				NameNode.Log.Error("BUG: unexpected exception ", e);
			}
		}

		/// <summary>
		/// updates quota without verification
		/// callers responsibility is to make sure quota is not exceeded
		/// </summary>
		internal static void UnprotectedUpdateCount(INodesInPath inodesInPath, int numOfINodes
			, QuotaCounts counts)
		{
			for (int i = 0; i < numOfINodes; i++)
			{
				if (inodesInPath.GetINode(i).IsQuotaSet())
				{
					// a directory with quota
					inodesInPath.GetINode(i).AsDirectory().GetDirectoryWithQuotaFeature().AddSpaceConsumed2Cache
						(counts);
				}
			}
		}

		public virtual EnumCounters<StorageType> GetStorageTypeDeltas(byte storagePolicyID
			, long dsDelta, short oldRep, short newRep)
		{
			EnumCounters<StorageType> typeSpaceDeltas = new EnumCounters<StorageType>(typeof(
				StorageType));
			// Storage type and its quota are only available when storage policy is set
			if (storagePolicyID != BlockStoragePolicySuite.IdUnspecified)
			{
				BlockStoragePolicy storagePolicy = GetBlockManager().GetStoragePolicy(storagePolicyID
					);
				if (oldRep != newRep)
				{
					IList<StorageType> oldChosenStorageTypes = storagePolicy.ChooseStorageTypes(oldRep
						);
					foreach (StorageType t in oldChosenStorageTypes)
					{
						if (!t.SupportTypeQuota())
						{
							continue;
						}
						Preconditions.CheckArgument(dsDelta > 0);
						typeSpaceDeltas.Add(t, -dsDelta);
					}
				}
				IList<StorageType> newChosenStorageTypes = storagePolicy.ChooseStorageTypes(newRep
					);
				foreach (StorageType t_1 in newChosenStorageTypes)
				{
					if (!t_1.SupportTypeQuota())
					{
						continue;
					}
					typeSpaceDeltas.Add(t_1, dsDelta);
				}
			}
			return typeSpaceDeltas;
		}

		/// <summary>Return the name of the path represented by inodes at [0, pos]</summary>
		internal static string GetFullPathName(INode[] inodes, int pos)
		{
			StringBuilder fullPathName = new StringBuilder();
			if (inodes[0].IsRoot())
			{
				if (pos == 0)
				{
					return Path.Separator;
				}
			}
			else
			{
				fullPathName.Append(inodes[0].GetLocalName());
			}
			for (int i = 1; i <= pos; i++)
			{
				fullPathName.Append(Path.SeparatorChar).Append(inodes[i].GetLocalName());
			}
			return fullPathName.ToString();
		}

		/// <returns>
		/// the relative path of an inode from one of its ancestors,
		/// represented by an array of inodes.
		/// </returns>
		private static INode[] GetRelativePathINodes(INode inode, INode ancestor)
		{
			// calculate the depth of this inode from the ancestor
			int depth = 0;
			for (INode i = inode; i != null && !i.Equals(ancestor); i = i.GetParent())
			{
				depth++;
			}
			INode[] inodes = new INode[depth];
			// fill up the inodes in the path from this inode to root
			for (int i_1 = 0; i_1 < depth; i_1++)
			{
				if (inode == null)
				{
					NameNode.stateChangeLog.Warn("Could not get full path." + " Corresponding file might have deleted already."
						);
					return null;
				}
				inodes[depth - i_1 - 1] = inode;
				inode = inode.GetParent();
			}
			return inodes;
		}

		private static INode[] GetFullPathINodes(INode inode)
		{
			return GetRelativePathINodes(inode, null);
		}

		/// <summary>Return the full path name of the specified inode</summary>
		internal static string GetFullPathName(INode inode)
		{
			INode[] inodes = GetFullPathINodes(inode);
			// inodes can be null only when its called without holding lock
			return inodes == null ? string.Empty : GetFullPathName(inodes, inodes.Length - 1);
		}

		/// <summary>Add the given child to the namespace.</summary>
		/// <param name="existing">the INodesInPath containing all the ancestral INodes</param>
		/// <param name="child">the new INode to add</param>
		/// <returns>
		/// a new INodesInPath instance containing the new child INode. Null
		/// if the adding fails.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">is thrown if it violates quota limit
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual INodesInPath AddINode(INodesInPath existing, INode child)
		{
			CacheName(child);
			WriteLock();
			try
			{
				return AddLastINode(existing, child, true);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>
		/// Verify quota for adding or moving a new INode with required
		/// namespace and storagespace to a given position.
		/// </summary>
		/// <param name="iip">INodes corresponding to a path</param>
		/// <param name="pos">position where a new INode will be added</param>
		/// <param name="deltas">needed namespace, storagespace and storage types</param>
		/// <param name="commonAncestor">
		/// Last node in inodes array that is a common ancestor
		/// for a INode that is being moved from one location to the other.
		/// Pass null if a node is not being moved.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">if quota limit is exceeded.
		/// 	</exception>
		internal static void VerifyQuota(INodesInPath iip, int pos, QuotaCounts deltas, INode
			 commonAncestor)
		{
			if (deltas.GetNameSpace() <= 0 && deltas.GetStorageSpace() <= 0 && deltas.GetTypeSpaces
				().AllLessOrEqual(0L))
			{
				// if quota is being freed or not being consumed
				return;
			}
			// check existing components in the path
			for (int i = (pos > iip.Length() ? iip.Length() : pos) - 1; i >= 0; i--)
			{
				if (commonAncestor == iip.GetINode(i))
				{
					// Stop checking for quota when common ancestor is reached
					return;
				}
				DirectoryWithQuotaFeature q = iip.GetINode(i).AsDirectory().GetDirectoryWithQuotaFeature
					();
				if (q != null)
				{
					// a directory with quota
					try
					{
						q.VerifyQuota(deltas);
					}
					catch (QuotaExceededException e)
					{
						IList<INode> inodes = iip.GetReadOnlyINodes();
						string path = GetFullPathName(Sharpen.Collections.ToArray(inodes, new INode[inodes
							.Count]), i);
						e.SetPathName(path);
						throw;
					}
				}
			}
		}

		/// <summary>Verify if the inode name is legal.</summary>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException"/>
		internal virtual void VerifyINodeName(byte[] childName)
		{
			if (Arrays.Equals(HdfsConstants.DotSnapshotDirBytes, childName))
			{
				string s = "\"" + HdfsConstants.DotSnapshotDir + "\" is a reserved name.";
				if (!namesystem.IsImageLoaded())
				{
					s += "  Please rename it before upgrade.";
				}
				throw new HadoopIllegalArgumentException(s);
			}
		}

		/// <summary>Verify child's name for fs limit.</summary>
		/// <param name="childName">byte[] containing new child name</param>
		/// <param name="parentPath">String containing parent path</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.FSLimitException.PathComponentTooLongException
		/// 	">child's name is too long.</exception>
		internal virtual void VerifyMaxComponentLength(byte[] childName, string parentPath
			)
		{
			if (maxComponentLength == 0)
			{
				return;
			}
			int length = childName.Length;
			if (length > maxComponentLength)
			{
				FSLimitException.PathComponentTooLongException e = new FSLimitException.PathComponentTooLongException
					(maxComponentLength, length, parentPath, DFSUtil.Bytes2String(childName));
				if (namesystem.IsImageLoaded())
				{
					throw e;
				}
				else
				{
					// Do not throw if edits log is still being processed
					NameNode.Log.Error("ERROR in FSDirectory.verifyINodeName", e);
				}
			}
		}

		/// <summary>Verify children size for fs limit.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.FSLimitException.MaxDirectoryItemsExceededException
		/// 	">too many children.</exception>
		internal virtual void VerifyMaxDirItems(INodeDirectory parent, string parentPath)
		{
			int count = parent.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).Size();
			if (count >= maxDirItems)
			{
				FSLimitException.MaxDirectoryItemsExceededException e = new FSLimitException.MaxDirectoryItemsExceededException
					(maxDirItems, count);
				if (namesystem.IsImageLoaded())
				{
					e.SetPathName(parentPath);
					throw e;
				}
				else
				{
					// Do not throw if edits log is still being processed
					NameNode.Log.Error("FSDirectory.verifyMaxDirItems: " + e.GetLocalizedMessage());
				}
			}
		}

		/// <summary>Add a child to the end of the path specified by INodesInPath.</summary>
		/// <returns>an INodesInPath instance containing the new INode</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		[VisibleForTesting]
		public virtual INodesInPath AddLastINode(INodesInPath existing, INode inode, bool
			 checkQuota)
		{
			System.Diagnostics.Debug.Assert(existing.GetLastINode() != null && existing.GetLastINode
				().IsDirectory());
			int pos = existing.Length();
			// Disallow creation of /.reserved. This may be created when loading
			// editlog/fsimage during upgrade since /.reserved was a valid name in older
			// release. This may also be called when a user tries to create a file
			// or directory /.reserved.
			if (pos == 1 && existing.GetINode(0) == rootDir && IsReservedName(inode))
			{
				throw new HadoopIllegalArgumentException("File name \"" + inode.GetLocalName() + 
					"\" is reserved and cannot " + "be created. If this is during upgrade change the name of the "
					 + "existing file or directory to another name before upgrading " + "to the new release."
					);
			}
			INodeDirectory parent = existing.GetINode(pos - 1).AsDirectory();
			// The filesystem limits are not really quotas, so this check may appear
			// odd. It's because a rename operation deletes the src, tries to add
			// to the dest, if that fails, re-adds the src from whence it came.
			// The rename code disables the quota when it's restoring to the
			// original location because a quota violation would cause the the item
			// to go "poof".  The fs limits must be bypassed for the same reason.
			if (checkQuota)
			{
				string parentPath = existing.GetPath();
				VerifyMaxComponentLength(inode.GetLocalNameBytes(), parentPath);
				VerifyMaxDirItems(parent, parentPath);
			}
			// always verify inode name
			VerifyINodeName(inode.GetLocalNameBytes());
			QuotaCounts counts = inode.ComputeQuotaUsage(GetBlockStoragePolicySuite());
			UpdateCount(existing, pos, counts, checkQuota);
			bool isRename = (inode.GetParent() != null);
			bool added;
			try
			{
				added = parent.AddChild(inode, true, existing.GetLatestSnapshotId());
			}
			catch (QuotaExceededException e)
			{
				UpdateCountNoQuotaCheck(existing, pos, counts.Negation());
				throw;
			}
			if (!added)
			{
				UpdateCountNoQuotaCheck(existing, pos, counts.Negation());
				return null;
			}
			else
			{
				if (!isRename)
				{
					AclStorage.CopyINodeDefaultAcl(inode);
				}
				AddToInodeMap(inode);
			}
			return INodesInPath.Append(existing, inode, inode.GetLocalNameBytes());
		}

		internal virtual INodesInPath AddLastINodeNoQuotaCheck(INodesInPath existing, INode
			 i)
		{
			try
			{
				return AddLastINode(existing, i, false);
			}
			catch (QuotaExceededException e)
			{
				NameNode.Log.Warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
			}
			return null;
		}

		/// <summary>Remove the last inode in the path from the namespace.</summary>
		/// <remarks>
		/// Remove the last inode in the path from the namespace.
		/// Note: the caller needs to update the ancestors' quota count.
		/// </remarks>
		/// <returns>
		/// -1 for failing to remove;
		/// 0 for removing a reference whose referred inode has other
		/// reference nodes;
		/// 1 otherwise.
		/// </returns>
		[VisibleForTesting]
		public virtual long RemoveLastINode(INodesInPath iip)
		{
			int latestSnapshot = iip.GetLatestSnapshotId();
			INode last = iip.GetLastINode();
			INodeDirectory parent = iip.GetINode(-2).AsDirectory();
			if (!parent.RemoveChild(last, latestSnapshot))
			{
				return -1;
			}
			return (!last.IsInLatestSnapshot(latestSnapshot) && INodeReference.TryRemoveReference
				(last) > 0) ? 0 : 1;
		}

		internal static string NormalizePath(string src)
		{
			if (src.Length > 1 && src.EndsWith("/"))
			{
				src = Sharpen.Runtime.Substring(src, 0, src.Length - 1);
			}
			return src;
		}

		[VisibleForTesting]
		public virtual long GetYieldCount()
		{
			return yieldCount;
		}

		internal virtual void AddYieldCount(long value)
		{
			yieldCount += value;
		}

		public virtual INodeMap GetINodeMap()
		{
			return inodeMap;
		}

		/// <summary>FSEditLogLoader implementation.</summary>
		/// <remarks>
		/// FSEditLogLoader implementation.
		/// Unlike FSNamesystem.truncate, this will not schedule block recovery.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void UnprotectedTruncate(string src, string clientName, string clientMachine
			, long newLength, long mtime, Block truncateBlock)
		{
			INodesInPath iip = GetINodesInPath(src, true);
			INodeFile file = iip.GetLastINode().AsFile();
			INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
			bool onBlockBoundary = UnprotectedTruncate(iip, newLength, collectedBlocks, mtime
				, null);
			if (!onBlockBoundary)
			{
				BlockInfoContiguous oldBlock = file.GetLastBlock();
				Block tBlk = GetFSNamesystem().PrepareFileForTruncate(iip, clientName, clientMachine
					, file.ComputeFileSize() - newLength, truncateBlock);
				System.Diagnostics.Debug.Assert(Block.MatchingIdAndGenStamp(tBlk, truncateBlock) 
					&& tBlk.GetNumBytes() == truncateBlock.GetNumBytes(), "Should be the same block."
					);
				if (oldBlock.GetBlockId() != tBlk.GetBlockId() && !file.IsBlockInLatestSnapshot(oldBlock
					))
				{
					GetBlockManager().RemoveBlockFromMap(oldBlock);
				}
			}
			System.Diagnostics.Debug.Assert(onBlockBoundary == (truncateBlock == null), "truncateBlock is null iff on block boundary: "
				 + truncateBlock);
			GetFSNamesystem().RemoveBlocksAndUpdateSafemodeTotal(collectedBlocks);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool Truncate(INodesInPath iip, long newLength, INode.BlocksMapUpdateInfo
			 collectedBlocks, long mtime, QuotaCounts delta)
		{
			WriteLock();
			try
			{
				return UnprotectedTruncate(iip, newLength, collectedBlocks, mtime, delta);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>
		/// Truncate has the following properties:
		/// 1.) Any block deletions occur now.
		/// </summary>
		/// <remarks>
		/// Truncate has the following properties:
		/// 1.) Any block deletions occur now.
		/// 2.) INode length is truncated now – new clients can only read up to
		/// the truncated length.
		/// 3.) INode will be set to UC and lastBlock set to UNDER_RECOVERY.
		/// 4.) NN will trigger DN truncation recovery and waits for DNs to report.
		/// 5.) File is considered UNDER_RECOVERY until truncation recovery completes.
		/// 6.) Soft and hard Lease expiration require truncation recovery to complete.
		/// </remarks>
		/// <returns>true if on the block boundary or false if recovery is need</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool UnprotectedTruncate(INodesInPath iip, long newLength, INode.BlocksMapUpdateInfo
			 collectedBlocks, long mtime, QuotaCounts delta)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			INodeFile file = iip.GetLastINode().AsFile();
			int latestSnapshot = iip.GetLatestSnapshotId();
			file.RecordModification(latestSnapshot, true);
			VerifyQuotaForTruncate(iip, file, newLength, delta);
			long remainingLength = file.CollectBlocksBeyondMax(newLength, collectedBlocks);
			file.ExcludeSnapshotBlocks(latestSnapshot, collectedBlocks);
			file.SetModificationTime(mtime);
			// return whether on a block boundary
			return (remainingLength - newLength) == 0;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		private void VerifyQuotaForTruncate(INodesInPath iip, INodeFile file, long newLength
			, QuotaCounts delta)
		{
			if (!GetFSNamesystem().IsImageLoaded() || ShouldSkipQuotaChecks())
			{
				// Do not check quota if edit log is still being processed
				return;
			}
			long diff = file.ComputeQuotaDeltaForTruncate(newLength);
			short repl = file.GetBlockReplication();
			delta.AddStorageSpace(diff * repl);
			BlockStoragePolicy policy = GetBlockStoragePolicySuite().GetPolicy(file.GetStoragePolicyID
				());
			IList<StorageType> types = policy.ChooseStorageTypes(repl);
			foreach (StorageType t in types)
			{
				if (t.SupportTypeQuota())
				{
					delta.AddTypeSpace(t, diff);
				}
			}
			if (diff > 0)
			{
				ReadLock();
				try
				{
					VerifyQuota(iip, iip.Length() - 1, delta, null);
				}
				finally
				{
					ReadUnlock();
				}
			}
		}

		/// <summary>This method is always called with writeLock of FSDirectory held.</summary>
		public void AddToInodeMap(INode inode)
		{
			if (inode is INodeWithAdditionalFields)
			{
				inodeMap.Put(inode);
				if (!inode.IsSymlink())
				{
					XAttrFeature xaf = inode.GetXAttrFeature();
					AddEncryptionZone((INodeWithAdditionalFields)inode, xaf);
				}
			}
		}

		private void AddEncryptionZone(INodeWithAdditionalFields inode, XAttrFeature xaf)
		{
			if (xaf == null)
			{
				return;
			}
			IList<XAttr> xattrs = xaf.GetXAttrs();
			foreach (XAttr xattr in xattrs)
			{
				string xaName = XAttrHelper.GetPrefixName(xattr);
				if (HdfsServerConstants.CryptoXattrEncryptionZone.Equals(xaName))
				{
					try
					{
						HdfsProtos.ZoneEncryptionInfoProto ezProto = HdfsProtos.ZoneEncryptionInfoProto.ParseFrom
							(xattr.GetValue());
						ezManager.UnprotectedAddEncryptionZone(inode.GetId(), PBHelper.Convert(ezProto.GetSuite
							()), PBHelper.Convert(ezProto.GetCryptoProtocolVersion()), ezProto.GetKeyName());
					}
					catch (InvalidProtocolBufferException)
					{
						NameNode.Log.Warn("Error parsing protocol buffer of " + "EZ XAttr " + xattr.GetName
							());
					}
				}
			}
		}

		/// <summary>
		/// This is to handle encryption zone for rootDir when loading from
		/// fsimage, and should only be called during NN restart.
		/// </summary>
		public void AddRootDirToEncryptionZone(XAttrFeature xaf)
		{
			AddEncryptionZone(rootDir, xaf);
		}

		/// <summary>This method is always called with writeLock of FSDirectory held.</summary>
		public void RemoveFromInodeMap<_T0>(IList<_T0> inodes)
			where _T0 : INode
		{
			if (inodes != null)
			{
				foreach (INode inode in inodes)
				{
					if (inode != null && inode is INodeWithAdditionalFields)
					{
						inodeMap.Remove(inode);
						ezManager.RemoveEncryptionZone(inode.GetId());
					}
				}
			}
		}

		/// <summary>Get the inode from inodeMap based on its inode id.</summary>
		/// <param name="id">The given id</param>
		/// <returns>The inode associated with the given id</returns>
		public virtual INode GetInode(long id)
		{
			ReadLock();
			try
			{
				return inodeMap.Get(id);
			}
			finally
			{
				ReadUnlock();
			}
		}

		[VisibleForTesting]
		internal virtual int GetInodeMapSize()
		{
			return inodeMap.Size();
		}

		internal virtual long TotalInodes()
		{
			ReadLock();
			try
			{
				return rootDir.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetNameSpace();
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Reset the entire namespace tree.</summary>
		internal virtual void Reset()
		{
			WriteLock();
			try
			{
				rootDir = CreateRoot(GetFSNamesystem());
				inodeMap.Clear();
				AddToInodeMap(rootDir);
				nameCache.Reset();
				inodeId.SetCurrentValue(INodeId.LastReservedId);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		internal virtual bool IsInAnEZ(INodesInPath iip)
		{
			ReadLock();
			try
			{
				return ezManager.IsInAnEZ(iip);
			}
			finally
			{
				ReadUnlock();
			}
		}

		internal virtual string GetKeyName(INodesInPath iip)
		{
			ReadLock();
			try
			{
				return ezManager.GetKeyName(iip);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual XAttr CreateEncryptionZone(string src, CipherSuite suite, CryptoProtocolVersion
			 version, string keyName)
		{
			WriteLock();
			try
			{
				return ezManager.CreateEncryptionZone(src, suite, version, keyName);
			}
			finally
			{
				WriteUnlock();
			}
		}

		internal virtual EncryptionZone GetEZForPath(INodesInPath iip)
		{
			ReadLock();
			try
			{
				return ezManager.GetEZINodeForPath(iip);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual BatchedRemoteIterator.BatchedListEntries<EncryptionZone> ListEncryptionZones
			(long prevId)
		{
			ReadLock();
			try
			{
				return ezManager.ListEncryptionZones(prevId);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Set the FileEncryptionInfo for an INode.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetFileEncryptionInfo(string src, FileEncryptionInfo info)
		{
			// Make the PB for the xattr
			HdfsProtos.PerFileEncryptionInfoProto proto = PBHelper.ConvertPerFileEncInfo(info
				);
			byte[] protoBytes = proto.ToByteArray();
			XAttr fileEncryptionAttr = XAttrHelper.BuildXAttr(HdfsServerConstants.CryptoXattrFileEncryptionInfo
				, protoBytes);
			IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(1);
			xAttrs.AddItem(fileEncryptionAttr);
			WriteLock();
			try
			{
				FSDirXAttrOp.UnprotectedSetXAttrs(this, src, xAttrs, EnumSet.Of(XAttrSetFlag.Create
					));
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>
		/// This function combines the per-file encryption info (obtained
		/// from the inode's XAttrs), and the encryption info from its zone, and
		/// returns a consolidated FileEncryptionInfo instance.
		/// </summary>
		/// <remarks>
		/// This function combines the per-file encryption info (obtained
		/// from the inode's XAttrs), and the encryption info from its zone, and
		/// returns a consolidated FileEncryptionInfo instance. Null is returned
		/// for non-encrypted files.
		/// </remarks>
		/// <param name="inode">inode of the file</param>
		/// <param name="snapshotId">
		/// ID of the snapshot that
		/// we want to get encryption info from
		/// </param>
		/// <param name="iip">
		/// inodes in the path containing the file, passed in to
		/// avoid obtaining the list of inodes again; if iip is
		/// null then the list of inodes will be obtained again
		/// </param>
		/// <returns>consolidated file encryption info; null for non-encrypted files</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FileEncryptionInfo GetFileEncryptionInfo(INode inode, int snapshotId
			, INodesInPath iip)
		{
			if (!inode.IsFile())
			{
				return null;
			}
			ReadLock();
			try
			{
				EncryptionZone encryptionZone = GetEZForPath(iip);
				if (encryptionZone == null)
				{
					// not an encrypted file
					return null;
				}
				else
				{
					if (encryptionZone.GetPath() == null || encryptionZone.GetPath().IsEmpty())
					{
						if (NameNode.Log.IsDebugEnabled())
						{
							NameNode.Log.Debug("Encryption zone " + encryptionZone.GetPath() + " does not have a valid path."
								);
						}
					}
				}
				CryptoProtocolVersion version = encryptionZone.GetVersion();
				CipherSuite suite = encryptionZone.GetSuite();
				string keyName = encryptionZone.GetKeyName();
				XAttr fileXAttr = FSDirXAttrOp.UnprotectedGetXAttrByName(inode, snapshotId, HdfsServerConstants
					.CryptoXattrFileEncryptionInfo);
				if (fileXAttr == null)
				{
					NameNode.Log.Warn("Could not find encryption XAttr for file " + iip.GetPath() + " in encryption zone "
						 + encryptionZone.GetPath());
					return null;
				}
				try
				{
					HdfsProtos.PerFileEncryptionInfoProto fileProto = HdfsProtos.PerFileEncryptionInfoProto
						.ParseFrom(fileXAttr.GetValue());
					return PBHelper.Convert(fileProto, suite, version, keyName);
				}
				catch (InvalidProtocolBufferException e)
				{
					throw new IOException("Could not parse file encryption info for " + "inode " + inode
						, e);
				}
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		internal static INode ResolveLastINode(INodesInPath iip)
		{
			INode inode = iip.GetLastINode();
			if (inode == null)
			{
				throw new FileNotFoundException("cannot find " + iip.GetPath());
			}
			return inode;
		}

		/// <summary>
		/// Caches frequently used file names to reuse file name objects and
		/// reduce heap size.
		/// </summary>
		internal virtual void CacheName(INode inode)
		{
			// Name is cached only for files
			if (!inode.IsFile())
			{
				return;
			}
			ByteArray name = new ByteArray(inode.GetLocalNameBytes());
			name = nameCache.Put(name);
			if (name != null)
			{
				inode.SetLocalName(name.GetBytes());
			}
		}

		internal virtual void Shutdown()
		{
			nameCache.Reset();
			inodeMap.Clear();
		}

		/// <summary>Given an INode get all the path complents leading to it from the root.</summary>
		/// <remarks>
		/// Given an INode get all the path complents leading to it from the root.
		/// If an Inode corresponding to C is given in /A/B/C, the returned
		/// patch components will be {root, A, B, C}.
		/// Note that this method cannot handle scenarios where the inode is in a
		/// snapshot.
		/// </remarks>
		public static byte[][] GetPathComponents(INode inode)
		{
			IList<byte[]> components = new AList<byte[]>();
			components.Add(0, inode.GetLocalNameBytes());
			while (inode.GetParent() != null)
			{
				components.Add(0, inode.GetParent().GetLocalNameBytes());
				inode = inode.GetParent();
			}
			return Sharpen.Collections.ToArray(components, new byte[components.Count][]);
		}

		/// <returns>path components for reserved path, else null.</returns>
		internal static byte[][] GetPathComponentsForReservedPath(string src)
		{
			return !IsReservedName(src) ? null : INode.GetPathComponents(src);
		}

		/// <summary>Check if a given inode name is reserved</summary>
		public static bool IsReservedName(INode inode)
		{
			return CheckReservedFileNames && Arrays.Equals(inode.GetLocalNameBytes(), DotReserved
				);
		}

		/// <summary>Check if a given path is reserved</summary>
		public static bool IsReservedName(string src)
		{
			return src.StartsWith(DotReservedPathPrefix + Path.Separator);
		}

		internal static bool IsReservedRawName(string src)
		{
			return src.StartsWith(DotReservedPathPrefix + Path.Separator + RawString);
		}

		/// <summary>Resolve a /.reserved/...</summary>
		/// <remarks>
		/// Resolve a /.reserved/... path to a non-reserved path.
		/// <p/>
		/// There are two special hierarchies under /.reserved/:
		/// <p/>
		/// /.reserved/.inodes/<inodeid> performs a path lookup by inodeid,
		/// <p/>
		/// /.reserved/raw/... returns the encrypted (raw) bytes of a file in an
		/// encryption zone. For instance, if /ezone is an encryption zone, then
		/// /ezone/a refers to the decrypted file and /.reserved/raw/ezone/a refers to
		/// the encrypted (raw) bytes of /ezone/a.
		/// <p/>
		/// Pathnames in the /.reserved/raw directory that resolve to files not in an
		/// encryption zone are equivalent to the corresponding non-raw path. Hence,
		/// if /a/b/c refers to a file that is not in an encryption zone, then
		/// /.reserved/raw/a/b/c is equivalent (they both refer to the same
		/// unencrypted file).
		/// </remarks>
		/// <param name="src">path that is being processed</param>
		/// <param name="pathComponents">path components corresponding to the path</param>
		/// <param name="fsd">FSDirectory</param>
		/// <returns>
		/// if the path indicates an inode, return path after replacing up to
		/// <inodeid> with the corresponding path of the inode, else the path
		/// in
		/// <paramref name="src"/>
		/// as is. If the path refers to a path in the "raw"
		/// directory, return the non-raw pathname.
		/// </returns>
		/// <exception cref="System.IO.FileNotFoundException">if inodeid is invalid</exception>
		internal static string ResolvePath(string src, byte[][] pathComponents, Org.Apache.Hadoop.Hdfs.Server.Namenode.FSDirectory
			 fsd)
		{
			int nComponents = (pathComponents == null) ? 0 : pathComponents.Length;
			if (nComponents <= 2)
			{
				return src;
			}
			if (!Arrays.Equals(DotReserved, pathComponents[1]))
			{
				/* This is not a /.reserved/ path so do nothing. */
				return src;
			}
			if (Arrays.Equals(DotInodes, pathComponents[2]))
			{
				/* It's a /.reserved/.inodes path. */
				if (nComponents > 3)
				{
					return ResolveDotInodesPath(src, pathComponents, fsd);
				}
				else
				{
					return src;
				}
			}
			else
			{
				if (Arrays.Equals(Raw, pathComponents[2]))
				{
					/* It's /.reserved/raw so strip off the /.reserved/raw prefix. */
					if (nComponents == 3)
					{
						return Path.Separator;
					}
					else
					{
						return ConstructRemainingPath(string.Empty, pathComponents, 3);
					}
				}
				else
				{
					/* It's some sort of /.reserved/<unknown> path. Ignore it. */
					return src;
				}
			}
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		private static string ResolveDotInodesPath(string src, byte[][] pathComponents, Org.Apache.Hadoop.Hdfs.Server.Namenode.FSDirectory
			 fsd)
		{
			string inodeId = DFSUtil.Bytes2String(pathComponents[3]);
			long id;
			try
			{
				id = long.Parse(inodeId);
			}
			catch (FormatException)
			{
				throw new FileNotFoundException("Invalid inode path: " + src);
			}
			if (id == INodeId.RootInodeId && pathComponents.Length == 4)
			{
				return Path.Separator;
			}
			INode inode = fsd.GetInode(id);
			if (inode == null)
			{
				throw new FileNotFoundException("File for given inode path does not exist: " + src
					);
			}
			// Handle single ".." for NFS lookup support.
			if ((pathComponents.Length > 4) && DFSUtil.Bytes2String(pathComponents[4]).Equals
				(".."))
			{
				INode parent = inode.GetParent();
				if (parent == null || parent.GetId() == INodeId.RootInodeId)
				{
					// inode is root, or its parent is root.
					return Path.Separator;
				}
				else
				{
					return parent.GetFullPathName();
				}
			}
			string path = string.Empty;
			if (id != INodeId.RootInodeId)
			{
				path = inode.GetFullPathName();
			}
			return ConstructRemainingPath(path, pathComponents, 4);
		}

		private static string ConstructRemainingPath(string pathPrefix, byte[][] pathComponents
			, int startAt)
		{
			StringBuilder path = new StringBuilder(pathPrefix);
			for (int i = startAt; i < pathComponents.Length; i++)
			{
				path.Append(Path.Separator).Append(DFSUtil.Bytes2String(pathComponents[i]));
			}
			if (NameNode.Log.IsDebugEnabled())
			{
				NameNode.Log.Debug("Resolved path is " + path);
			}
			return path.ToString();
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual INode GetINode4DotSnapshot(string src)
		{
			Preconditions.CheckArgument(src.EndsWith(HdfsConstants.SeparatorDotSnapshotDir), 
				"%s does not end with %s", src, HdfsConstants.SeparatorDotSnapshotDir);
			string dirPath = NormalizePath(Sharpen.Runtime.Substring(src, 0, src.Length - HdfsConstants
				.DotSnapshotDir.Length));
			INode node = this.GetINode(dirPath);
			if (node != null && node.IsDirectory() && node.AsDirectory().IsSnapshottable())
			{
				return node;
			}
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual INodesInPath GetExistingPathINodes(byte[][] components)
		{
			return INodesInPath.Resolve(rootDir, components, false);
		}

		/// <summary>
		/// Get
		/// <see cref="INode"/>
		/// associated with the file / directory.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		public virtual INodesInPath GetINodesInPath4Write(string src)
		{
			return GetINodesInPath4Write(src, true);
		}

		/// <summary>
		/// Get
		/// <see cref="INode"/>
		/// associated with the file / directory.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException">if path is in RO snapshot
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual INode GetINode4Write(string src)
		{
			return GetINodesInPath4Write(src, true).GetLastINode();
		}

		/// <returns>
		/// the
		/// <see cref="INodesInPath"/>
		/// containing all inodes in the path.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual INodesInPath GetINodesInPath(string path, bool resolveLink)
		{
			byte[][] components = INode.GetPathComponents(path);
			return INodesInPath.Resolve(rootDir, components, resolveLink);
		}

		/// <returns>the last inode in the path.</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual INode GetINode(string path, bool resolveLink)
		{
			return GetINodesInPath(path, resolveLink).GetLastINode();
		}

		/// <summary>
		/// Get
		/// <see cref="INode"/>
		/// associated with the file / directory.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual INode GetINode(string src)
		{
			return GetINode(src, true);
		}

		/// <returns>the INodesInPath of the components in src</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if symlink can't be resolved
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException">if path is in RO snapshot
		/// 	</exception>
		internal virtual INodesInPath GetINodesInPath4Write(string src, bool resolveLink)
		{
			byte[][] components = INode.GetPathComponents(src);
			INodesInPath inodesInPath = INodesInPath.Resolve(rootDir, components, resolveLink
				);
			if (inodesInPath.IsSnapshot())
			{
				throw new SnapshotAccessControlException("Modification on a read-only snapshot is disallowed"
					);
			}
			return inodesInPath;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual FSPermissionChecker GetPermissionChecker()
		{
			try
			{
				return GetPermissionChecker(fsOwnerShortUserName, supergroup, NameNode.GetRemoteUser
					());
			}
			catch (IOException e)
			{
				throw new AccessControlException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		[VisibleForTesting]
		internal virtual FSPermissionChecker GetPermissionChecker(string fsOwner, string 
			superGroup, UserGroupInformation ugi)
		{
			return new FSPermissionChecker(fsOwner, superGroup, ugi, attributeProvider == null
				 ? DefaultINodeAttributesProvider.DefaultProvider : attributeProvider);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckOwner(FSPermissionChecker pc, INodesInPath iip)
		{
			CheckPermission(pc, iip, true, null, null, null, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckPathAccess(FSPermissionChecker pc, INodesInPath iip, FsAction
			 access)
		{
			CheckPermission(pc, iip, false, null, null, access, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckParentAccess(FSPermissionChecker pc, INodesInPath iip, 
			FsAction access)
		{
			CheckPermission(pc, iip, false, null, access, null, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckAncestorAccess(FSPermissionChecker pc, INodesInPath iip
			, FsAction access)
		{
			CheckPermission(pc, iip, false, access, null, null, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckTraverse(FSPermissionChecker pc, INodesInPath iip)
		{
			CheckPermission(pc, iip, false, null, null, null, null);
		}

		/// <summary>Check whether current user have permissions to access the path.</summary>
		/// <remarks>
		/// Check whether current user have permissions to access the path. For more
		/// details of the parameters, see
		/// <see cref="FSPermissionChecker.CheckPermission(CachePool, Org.Apache.Hadoop.FS.Permission.FsAction)
		/// 	"/>
		/// .
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckPermission(FSPermissionChecker pc, INodesInPath iip, bool
			 doCheckOwner, FsAction ancestorAccess, FsAction parentAccess, FsAction access, 
			FsAction subAccess)
		{
			CheckPermission(pc, iip, doCheckOwner, ancestorAccess, parentAccess, access, subAccess
				, false);
		}

		/// <summary>Check whether current user have permissions to access the path.</summary>
		/// <remarks>
		/// Check whether current user have permissions to access the path. For more
		/// details of the parameters, see
		/// <see cref="FSPermissionChecker.CheckPermission(CachePool, Org.Apache.Hadoop.FS.Permission.FsAction)
		/// 	"/>
		/// .
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual void CheckPermission(FSPermissionChecker pc, INodesInPath iip, bool
			 doCheckOwner, FsAction ancestorAccess, FsAction parentAccess, FsAction access, 
			FsAction subAccess, bool ignoreEmptyDir)
		{
			if (!pc.IsSuperUser())
			{
				ReadLock();
				try
				{
					pc.CheckPermission(iip, doCheckOwner, ancestorAccess, parentAccess, access, subAccess
						, ignoreEmptyDir);
				}
				finally
				{
					ReadUnlock();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual HdfsFileStatus GetAuditFileInfo(INodesInPath iip)
		{
			return (namesystem.IsAuditEnabled() && namesystem.IsExternalInvocation()) ? FSDirStatAndListingOp
				.GetFileInfo(this, iip.GetPath(), iip, false, false) : null;
		}

		/// <summary>Verify that parent directory of src exists.</summary>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		internal virtual void VerifyParentDir(INodesInPath iip, string src)
		{
			Path parent = new Path(src).GetParent();
			if (parent != null)
			{
				INode parentNode = iip.GetINode(-2);
				if (parentNode == null)
				{
					throw new FileNotFoundException("Parent directory doesn't exist: " + parent);
				}
				else
				{
					if (!parentNode.IsDirectory() && !parentNode.IsSymlink())
					{
						throw new ParentNotDirectoryException("Parent path is not a directory: " + parent
							);
					}
				}
			}
		}

		/// <summary>Allocate a new inode ID.</summary>
		internal virtual long AllocateNewInodeId()
		{
			return inodeId.NextValue();
		}

		/// <returns>the last inode ID.</returns>
		public virtual long GetLastInodeId()
		{
			return inodeId.GetCurrentValue();
		}

		/// <summary>Set the last allocated inode id when fsimage or editlog is loaded.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ResetLastInodeId(long newValue)
		{
			try
			{
				inodeId.SkipTo(newValue);
			}
			catch (InvalidOperationException ise)
			{
				throw new IOException(ise);
			}
		}

		/// <summary>Should only be used for tests to reset to any value</summary>
		internal virtual void ResetLastInodeIdWithoutChecking(long newValue)
		{
			inodeId.SetCurrentValue(newValue);
		}

		internal virtual INodeAttributes GetAttributes(string fullPath, byte[] path, INode
			 node, int snapshot)
		{
			INodeAttributes nodeAttrs = node;
			if (attributeProvider != null)
			{
				nodeAttrs = node.GetSnapshotINode(snapshot);
				fullPath = fullPath + (fullPath.EndsWith(Path.Separator) ? string.Empty : Path.Separator
					) + DFSUtil.Bytes2String(path);
				nodeAttrs = attributeProvider.GetAttributes(fullPath, nodeAttrs);
			}
			else
			{
				nodeAttrs = node.GetSnapshotINode(snapshot);
			}
			return nodeAttrs;
		}
	}
}

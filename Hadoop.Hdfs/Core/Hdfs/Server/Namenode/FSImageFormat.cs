using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Contains inner classes for reading or writing the on-disk format for
	/// FSImages.
	/// </summary>
	/// <remarks>
	/// Contains inner classes for reading or writing the on-disk format for
	/// FSImages.
	/// In particular, the format of the FSImage looks like:
	/// <pre>
	/// FSImage {
	/// layoutVersion: int, namespaceID: int, numberItemsInFSDirectoryTree: long,
	/// namesystemGenerationStampV1: long, namesystemGenerationStampV2: long,
	/// generationStampAtBlockIdSwitch:long, lastAllocatedBlockId:
	/// long transactionID: long, snapshotCounter: int, numberOfSnapshots: int,
	/// numOfSnapshottableDirs: int,
	/// {FSDirectoryTree, FilesUnderConstruction, SecretManagerState} (can be compressed)
	/// }
	/// FSDirectoryTree (if
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.FsimageNameOptimization
	/// 	"/>
	/// is supported) {
	/// INodeInfo of root, numberOfChildren of root: int
	/// [list of INodeInfo of root's children],
	/// [list of INodeDirectoryInfo of root's directory children]
	/// }
	/// FSDirectoryTree (if
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.FsimageNameOptimization
	/// 	"/>
	/// not supported){
	/// [list of INodeInfo of INodes in topological order]
	/// }
	/// INodeInfo {
	/// {
	/// localName: short + byte[]
	/// } when
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.FsimageNameOptimization
	/// 	"/>
	/// is supported
	/// or
	/// {
	/// fullPath: byte[]
	/// } when
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.FsimageNameOptimization
	/// 	"/>
	/// is not supported
	/// replicationFactor: short, modificationTime: long,
	/// accessTime: long, preferredBlockSize: long,
	/// numberOfBlocks: int (-1 for INodeDirectory, -2 for INodeSymLink),
	/// {
	/// nsQuota: long, dsQuota: long,
	/// {
	/// isINodeSnapshottable: byte,
	/// isINodeWithSnapshot: byte (if isINodeSnapshottable is false)
	/// } (when
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.Snapshot"/>
	/// is supported),
	/// fsPermission: short, PermissionStatus
	/// } for INodeDirectory
	/// or
	/// {
	/// symlinkString, fsPermission: short, PermissionStatus
	/// } for INodeSymlink
	/// or
	/// {
	/// [list of BlockInfo]
	/// [list of FileDiff]
	/// {
	/// isINodeFileUnderConstructionSnapshot: byte,
	/// {clientName: short + byte[], clientMachine: short + byte[]} (when
	/// isINodeFileUnderConstructionSnapshot is true),
	/// } (when
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.Snapshot"/>
	/// is supported and writing snapshotINode),
	/// fsPermission: short, PermissionStatus
	/// } for INodeFile
	/// }
	/// INodeDirectoryInfo {
	/// fullPath of the directory: short + byte[],
	/// numberOfChildren: int, [list of INodeInfo of children INode],
	/// {
	/// numberOfSnapshots: int,
	/// [list of Snapshot] (when NumberOfSnapshots is positive),
	/// numberOfDirectoryDiffs: int,
	/// [list of DirectoryDiff] (NumberOfDirectoryDiffs is positive),
	/// number of children that are directories,
	/// [list of INodeDirectoryInfo of the directory children] (includes
	/// snapshot copies of deleted sub-directories)
	/// } (when
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LayoutVersion.Feature.Snapshot"/>
	/// is supported),
	/// }
	/// Snapshot {
	/// snapshotID: int, root of Snapshot: INodeDirectoryInfo (its local name is
	/// the name of the snapshot)
	/// }
	/// DirectoryDiff {
	/// full path of the root of the associated Snapshot: short + byte[],
	/// childrenSize: int,
	/// isSnapshotRoot: byte,
	/// snapshotINodeIsNotNull: byte (when isSnapshotRoot is false),
	/// snapshotINode: INodeDirectory (when SnapshotINodeIsNotNull is true), Diff
	/// }
	/// Diff {
	/// createdListSize: int, [Local name of INode in created list],
	/// deletedListSize: int, [INode in deleted list: INodeInfo]
	/// }
	/// FileDiff {
	/// full path of the root of the associated Snapshot: short + byte[],
	/// fileSize: long,
	/// snapshotINodeIsNotNull: byte,
	/// snapshotINode: INodeFile (when SnapshotINodeIsNotNull is true), Diff
	/// }
	/// </pre>
	/// </remarks>
	public class FSImageFormat
	{
		private static readonly Log Log = FSImage.Log;

		private FSImageFormat()
		{
		}

		internal interface AbstractLoader
		{
			// Static-only class
			MD5Hash GetLoadedImageMd5();

			long GetLoadedImageTxId();
		}

		internal class LoaderDelegator : FSImageFormat.AbstractLoader
		{
			private FSImageFormat.AbstractLoader impl;

			private readonly Configuration conf;

			private readonly FSNamesystem fsn;

			internal LoaderDelegator(Configuration conf, FSNamesystem fsn)
			{
				this.conf = conf;
				this.fsn = fsn;
			}

			public virtual MD5Hash GetLoadedImageMd5()
			{
				return impl.GetLoadedImageMd5();
			}

			public virtual long GetLoadedImageTxId()
			{
				return impl.GetLoadedImageTxId();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Load(FilePath file, bool requireSameLayoutVersion)
			{
				Preconditions.CheckState(impl == null, "Image already loaded!");
				FileInputStream @is = null;
				try
				{
					@is = new FileInputStream(file);
					byte[] magic = new byte[FSImageUtil.MagicHeader.Length];
					IOUtils.ReadFully(@is, magic, 0, magic.Length);
					if (Arrays.Equals(magic, FSImageUtil.MagicHeader))
					{
						FSImageFormatProtobuf.Loader loader = new FSImageFormatProtobuf.Loader(conf, fsn, 
							requireSameLayoutVersion);
						impl = loader;
						loader.Load(file);
					}
					else
					{
						FSImageFormat.Loader loader = new FSImageFormat.Loader(conf, fsn);
						impl = loader;
						loader.Load(file);
					}
				}
				finally
				{
					IOUtils.Cleanup(Log, @is);
				}
			}
		}

		/// <summary>Construct a loader class to load the image.</summary>
		/// <remarks>
		/// Construct a loader class to load the image. It chooses the loader based on
		/// the layout version.
		/// </remarks>
		public static FSImageFormat.LoaderDelegator NewLoader(Configuration conf, FSNamesystem
			 fsn)
		{
			return new FSImageFormat.LoaderDelegator(conf, fsn);
		}

		/// <summary>A one-shot class responsible for loading an image.</summary>
		/// <remarks>
		/// A one-shot class responsible for loading an image. The load() function
		/// should be called once, after which the getter methods may be used to retrieve
		/// information about the image that was loaded, if loading was successful.
		/// </remarks>
		public class Loader : FSImageFormat.AbstractLoader
		{
			private readonly Configuration conf;

			/// <summary>which namesystem this loader is working for</summary>
			private readonly FSNamesystem namesystem;

			/// <summary>Set to true once a file has been loaded using this loader.</summary>
			private bool loaded = false;

			/// <summary>The transaction ID of the last edit represented by the loaded file</summary>
			private long imgTxId;

			/// <summary>The MD5 sum of the loaded file</summary>
			private MD5Hash imgDigest;

			private IDictionary<int, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				> snapshotMap = null;

			private readonly SnapshotFSImageFormat.ReferenceMap referenceMap = new SnapshotFSImageFormat.ReferenceMap
				();

			internal Loader(Configuration conf, FSNamesystem namesystem)
			{
				this.conf = conf;
				this.namesystem = namesystem;
			}

			/// <summary>Return the MD5 checksum of the image that has been loaded.</summary>
			/// <exception cref="System.InvalidOperationException">if load() has not yet been called.
			/// 	</exception>
			public virtual MD5Hash GetLoadedImageMd5()
			{
				CheckLoaded();
				return imgDigest;
			}

			public virtual long GetLoadedImageTxId()
			{
				CheckLoaded();
				return imgTxId;
			}

			/// <summary>Throw IllegalStateException if load() has not yet been called.</summary>
			private void CheckLoaded()
			{
				if (!loaded)
				{
					throw new InvalidOperationException("Image not yet loaded!");
				}
			}

			/// <summary>Throw IllegalStateException if load() has already been called.</summary>
			private void CheckNotLoaded()
			{
				if (loaded)
				{
					throw new InvalidOperationException("Image already loaded!");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Load(FilePath curFile)
			{
				CheckNotLoaded();
				System.Diagnostics.Debug.Assert(curFile != null, "curFile is null");
				StartupProgress prog = NameNode.GetStartupProgress();
				Step step = new Step(StepType.Inodes);
				prog.BeginStep(Phase.LoadingFsimage, step);
				long startTime = Time.MonotonicNow();
				//
				// Load in bits
				//
				MessageDigest digester = MD5Hash.GetDigester();
				DigestInputStream fin = new DigestInputStream(new FileInputStream(curFile), digester
					);
				DataInputStream @in = new DataInputStream(fin);
				try
				{
					// read image version: first appeared in version -1
					int imgVersion = @in.ReadInt();
					if (GetLayoutVersion() != imgVersion)
					{
						throw new InconsistentFSStateException(curFile, "imgVersion " + imgVersion + " expected to be "
							 + GetLayoutVersion());
					}
					bool supportSnapshot = NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot
						, imgVersion);
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddLayoutFlags, imgVersion
						))
					{
						LayoutFlags.Read(@in);
					}
					// read namespaceID: first appeared in version -2
					@in.ReadInt();
					long numFiles = @in.ReadLong();
					// read in the last generation stamp for legacy blocks.
					long genstamp = @in.ReadLong();
					namesystem.GetBlockIdManager().SetGenerationStampV1(genstamp);
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.SequentialBlockId, imgVersion
						))
					{
						// read the starting generation stamp for sequential block IDs
						genstamp = @in.ReadLong();
						namesystem.GetBlockIdManager().SetGenerationStampV2(genstamp);
						// read the last generation stamp for blocks created after
						// the switch to sequential block IDs.
						long stampAtIdSwitch = @in.ReadLong();
						namesystem.GetBlockIdManager().SetGenerationStampV1Limit(stampAtIdSwitch);
						// read the max sequential block ID.
						long maxSequentialBlockId = @in.ReadLong();
						namesystem.GetBlockIdManager().SetLastAllocatedBlockId(maxSequentialBlockId);
					}
					else
					{
						long startingGenStamp = namesystem.GetBlockIdManager().UpgradeGenerationStampToV2
							();
						// This is an upgrade.
						Log.Info("Upgrading to sequential block IDs. Generation stamp " + "for new blocks set to "
							 + startingGenStamp);
					}
					// read the transaction ID of the last edit represented by
					// this image
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.StoredTxids, imgVersion))
					{
						imgTxId = @in.ReadLong();
					}
					else
					{
						imgTxId = 0;
					}
					// read the last allocated inode id in the fsimage
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, imgVersion))
					{
						long lastInodeId = @in.ReadLong();
						namesystem.dir.ResetLastInodeId(lastInodeId);
						if (Log.IsDebugEnabled())
						{
							Log.Debug("load last allocated InodeId from fsimage:" + lastInodeId);
						}
					}
					else
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Old layout version doesn't have inode id." + " Will assign new id for each inode."
								);
						}
					}
					if (supportSnapshot)
					{
						snapshotMap = namesystem.GetSnapshotManager().Read(@in, this);
					}
					// read compression related info
					FSImageCompression compression;
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FsimageCompression, imgVersion
						))
					{
						compression = FSImageCompression.ReadCompressionHeader(conf, @in);
					}
					else
					{
						compression = FSImageCompression.CreateNoopCompression();
					}
					@in = compression.UnwrapInputStream(fin);
					Log.Info("Loading image file " + curFile + " using " + compression);
					// load all inodes
					Log.Info("Number of files = " + numFiles);
					prog.SetTotal(Phase.LoadingFsimage, step, numFiles);
					StartupProgress.Counter counter = prog.GetCounter(Phase.LoadingFsimage, step);
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FsimageNameOptimization, 
						imgVersion))
					{
						if (supportSnapshot)
						{
							LoadLocalNameINodesWithSnapshot(numFiles, @in, counter);
						}
						else
						{
							LoadLocalNameINodes(numFiles, @in, counter);
						}
					}
					else
					{
						LoadFullNameINodes(numFiles, @in, counter);
					}
					LoadFilesUnderConstruction(@in, supportSnapshot, counter);
					prog.EndStep(Phase.LoadingFsimage, step);
					// Now that the step is finished, set counter equal to total to adjust
					// for possible under-counting due to reference inodes.
					prog.SetCount(Phase.LoadingFsimage, step, numFiles);
					LoadSecretManagerState(@in);
					LoadCacheManagerState(@in);
					// make sure to read to the end of file
					bool eof = (@in.Read() == -1);
					System.Diagnostics.Debug.Assert(eof, "Should have reached the end of image file "
						 + curFile);
				}
				finally
				{
					@in.Close();
				}
				imgDigest = new MD5Hash(digester.Digest());
				loaded = true;
				Log.Info("Image file " + curFile + " of size " + curFile.Length() + " bytes loaded in "
					 + (Time.MonotonicNow() - startTime) / 1000 + " seconds.");
			}

			/// <summary>Update the root node's attributes</summary>
			private void UpdateRootAttr(INodeWithAdditionalFields root)
			{
				QuotaCounts q = root.GetQuotaCounts();
				long nsQuota = q.GetNameSpace();
				long dsQuota = q.GetStorageSpace();
				FSDirectory fsDir = namesystem.dir;
				if (nsQuota != -1 || dsQuota != -1)
				{
					fsDir.rootDir.GetDirectoryWithQuotaFeature().SetQuota(nsQuota, dsQuota);
				}
				fsDir.rootDir.CloneModificationTime(root);
				fsDir.rootDir.ClonePermissionStatus(root);
			}

			/// <summary>
			/// Load fsimage files when 1) only local names are stored,
			/// and 2) snapshot is supported.
			/// </summary>
			/// <param name="numFiles">number of files expected to be read</param>
			/// <param name="in">Image input stream</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException"/>
			private void LoadLocalNameINodesWithSnapshot(long numFiles, DataInput @in, StartupProgress.Counter
				 counter)
			{
				System.Diagnostics.Debug.Assert(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
					.FsimageNameOptimization, GetLayoutVersion()));
				System.Diagnostics.Debug.Assert(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
					.Snapshot, GetLayoutVersion()));
				// load root
				LoadRoot(@in, counter);
				// load rest of the nodes recursively
				LoadDirectoryWithSnapshot(@in, counter);
			}

			/// <summary>load fsimage files assuming only local names are stored.</summary>
			/// <remarks>
			/// load fsimage files assuming only local names are stored. Used when
			/// snapshots are not supported by the layout version.
			/// </remarks>
			/// <param name="numFiles">number of files expected to be read</param>
			/// <param name="in">image input stream</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException"/>
			private void LoadLocalNameINodes(long numFiles, DataInput @in, StartupProgress.Counter
				 counter)
			{
				System.Diagnostics.Debug.Assert(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
					.FsimageNameOptimization, GetLayoutVersion()));
				System.Diagnostics.Debug.Assert(numFiles > 0);
				// load root
				LoadRoot(@in, counter);
				// have loaded the first file (the root)
				numFiles--;
				// load rest of the nodes directory by directory
				while (numFiles > 0)
				{
					numFiles -= LoadDirectory(@in, counter);
				}
				if (numFiles != 0)
				{
					throw new IOException("Read unexpect number of files: " + -numFiles);
				}
			}

			/// <summary>
			/// Load information about root, and use the information to update the root
			/// directory of NameSystem.
			/// </summary>
			/// <param name="in">
			/// The
			/// <see cref="System.IO.DataInput"/>
			/// instance to read.
			/// </param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException"/>
			private void LoadRoot(DataInput @in, StartupProgress.Counter counter)
			{
				// load root
				if (@in.ReadShort() != 0)
				{
					throw new IOException("First node is not root");
				}
				INodeDirectory root = LoadINode(null, false, @in, counter).AsDirectory();
				// update the root's attributes
				UpdateRootAttr(root);
			}

			/// <summary>Load children nodes for the parent directory.</summary>
			/// <exception cref="System.IO.IOException"/>
			private int LoadChildren(INodeDirectory parent, DataInput @in, StartupProgress.Counter
				 counter)
			{
				int numChildren = @in.ReadInt();
				for (int i = 0; i < numChildren; i++)
				{
					// load single inode
					INode newNode = LoadINodeWithLocalName(false, @in, true, counter);
					AddToParent(parent, newNode);
				}
				return numChildren;
			}

			/// <summary>Load a directory when snapshot is supported.</summary>
			/// <param name="in">
			/// The
			/// <see cref="System.IO.DataInput"/>
			/// instance to read.
			/// </param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException"/>
			private void LoadDirectoryWithSnapshot(DataInput @in, StartupProgress.Counter counter
				)
			{
				// Step 1. Identify the parent INode
				long inodeId = @in.ReadLong();
				INodeDirectory parent = this.namesystem.dir.GetInode(inodeId).AsDirectory();
				// Check if the whole subtree has been saved (for reference nodes)
				bool toLoadSubtree = referenceMap.ToProcessSubtree(parent.GetId());
				if (!toLoadSubtree)
				{
					return;
				}
				// Step 2. Load snapshots if parent is snapshottable
				int numSnapshots = @in.ReadInt();
				if (numSnapshots >= 0)
				{
					// load snapshots and snapshotQuota
					SnapshotFSImageFormat.LoadSnapshotList(parent, numSnapshots, @in, this);
					if (parent.GetDirectorySnapshottableFeature().GetSnapshotQuota() > 0)
					{
						// add the directory to the snapshottable directory list in 
						// SnapshotManager. Note that we only add root when its snapshot quota
						// is positive.
						this.namesystem.GetSnapshotManager().AddSnapshottable(parent);
					}
				}
				// Step 3. Load children nodes under parent
				LoadChildren(parent, @in, counter);
				// Step 4. load Directory Diff List
				SnapshotFSImageFormat.LoadDirectoryDiffList(parent, @in, this);
				// Recursively load sub-directories, including snapshot copies of deleted
				// directories
				int numSubTree = @in.ReadInt();
				for (int i = 0; i < numSubTree; i++)
				{
					LoadDirectoryWithSnapshot(@in, counter);
				}
			}

			/// <summary>Load all children of a directory</summary>
			/// <param name="in">input to load from</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <returns>number of child inodes read</returns>
			/// <exception cref="System.IO.IOException"/>
			private int LoadDirectory(DataInput @in, StartupProgress.Counter counter)
			{
				string parentPath = FSImageSerialization.ReadString(@in);
				// Rename .snapshot paths if we're doing an upgrade
				parentPath = RenameReservedPathsOnUpgrade(parentPath, GetLayoutVersion());
				INodeDirectory parent = INodeDirectory.ValueOf(namesystem.dir.GetINode(parentPath
					, true), parentPath);
				return LoadChildren(parent, @in, counter);
			}

			/// <summary>load fsimage files assuming full path names are stored</summary>
			/// <param name="numFiles">total number of files to load</param>
			/// <param name="in">data input stream</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException">if any error occurs</exception>
			private void LoadFullNameINodes(long numFiles, DataInput @in, StartupProgress.Counter
				 counter)
			{
				byte[][] pathComponents;
				byte[][] parentPath = new byte[][] { new byte[] {  } };
				FSDirectory fsDir = namesystem.dir;
				INodeDirectory parentINode = fsDir.rootDir;
				for (long i = 0; i < numFiles; i++)
				{
					pathComponents = FSImageSerialization.ReadPathComponents(@in);
					for (int j = 0; j < pathComponents.Length; j++)
					{
						byte[] newComponent = RenameReservedComponentOnUpgrade(pathComponents[j], GetLayoutVersion
							());
						if (!Arrays.Equals(newComponent, pathComponents[j]))
						{
							string oldPath = DFSUtil.ByteArray2PathString(pathComponents);
							pathComponents[j] = newComponent;
							string newPath = DFSUtil.ByteArray2PathString(pathComponents);
							Log.Info("Renaming reserved path " + oldPath + " to " + newPath);
						}
					}
					INode newNode = LoadINode(pathComponents[pathComponents.Length - 1], false, @in, 
						counter);
					if (IsRoot(pathComponents))
					{
						// it is the root
						// update the root's attributes
						UpdateRootAttr(newNode.AsDirectory());
						continue;
					}
					namesystem.dir.AddToInodeMap(newNode);
					// check if the new inode belongs to the same parent
					if (!IsParent(pathComponents, parentPath))
					{
						parentINode = GetParentINodeDirectory(pathComponents);
						parentPath = GetParent(pathComponents);
					}
					// add new inode
					AddToParent(parentINode, newNode);
				}
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.PathIsNotDirectoryException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			private INodeDirectory GetParentINodeDirectory(byte[][] pathComponents)
			{
				if (pathComponents.Length < 2)
				{
					// root
					return null;
				}
				// Gets the parent INode
				INodesInPath inodes = namesystem.dir.GetExistingPathINodes(pathComponents);
				return INodeDirectory.ValueOf(inodes.GetINode(-2), pathComponents);
			}

			/// <summary>Add the child node to parent and, if child is a file, update block map.</summary>
			/// <remarks>
			/// Add the child node to parent and, if child is a file, update block map.
			/// This method is only used for image loading so that synchronization,
			/// modification time update and space count update are not needed.
			/// </remarks>
			private void AddToParent(INodeDirectory parent, INode child)
			{
				FSDirectory fsDir = namesystem.dir;
				if (parent == fsDir.rootDir)
				{
					child.SetLocalName(RenameReservedRootComponentOnUpgrade(child.GetLocalNameBytes()
						, GetLayoutVersion()));
				}
				// NOTE: This does not update space counts for parents
				if (!parent.AddChild(child))
				{
					return;
				}
				namesystem.dir.CacheName(child);
				if (child.IsFile())
				{
					UpdateBlocksMap(child.AsFile());
				}
			}

			public virtual void UpdateBlocksMap(INodeFile file)
			{
				// Add file->block mapping
				BlockInfoContiguous[] blocks = file.GetBlocks();
				if (blocks != null)
				{
					BlockManager bm = namesystem.GetBlockManager();
					for (int i = 0; i < blocks.Length; i++)
					{
						file.SetBlock(i, bm.AddBlockCollection(blocks[i], file));
					}
				}
			}

			/// <returns>The FSDirectory of the namesystem where the fsimage is loaded</returns>
			public virtual FSDirectory GetFSDirectoryInLoading()
			{
				return namesystem.dir;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual INode LoadINodeWithLocalName(bool isSnapshotINode, DataInput @in, 
				bool updateINodeMap)
			{
				return LoadINodeWithLocalName(isSnapshotINode, @in, updateINodeMap, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual INode LoadINodeWithLocalName(bool isSnapshotINode, DataInput @in, 
				bool updateINodeMap, StartupProgress.Counter counter)
			{
				byte[] localName = FSImageSerialization.ReadLocalName(@in);
				localName = RenameReservedComponentOnUpgrade(localName, GetLayoutVersion());
				INode inode = LoadINode(localName, isSnapshotINode, @in, counter);
				if (updateINodeMap)
				{
					namesystem.dir.AddToInodeMap(inode);
				}
				return inode;
			}

			/// <summary>load an inode from fsimage except for its name</summary>
			/// <param name="in">data input stream from which image is read</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <returns>an inode</returns>
			/// <exception cref="System.IO.IOException"/>
			internal virtual INode LoadINode(byte[] localName, bool isSnapshotINode, DataInput
				 @in, StartupProgress.Counter counter)
			{
				int imgVersion = GetLayoutVersion();
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot, imgVersion))
				{
					namesystem.GetFSDirectory().VerifyINodeName(localName);
				}
				long inodeId = NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, imgVersion
					) ? @in.ReadLong() : namesystem.dir.AllocateNewInodeId();
				short replication = namesystem.GetBlockManager().AdjustReplication(@in.ReadShort(
					));
				long modificationTime = @in.ReadLong();
				long atime = 0;
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FileAccessTime, imgVersion
					))
				{
					atime = @in.ReadLong();
				}
				long blockSize = @in.ReadLong();
				int numBlocks = @in.ReadInt();
				if (numBlocks >= 0)
				{
					// file
					// read blocks
					BlockInfoContiguous[] blocks = new BlockInfoContiguous[numBlocks];
					for (int j = 0; j < numBlocks; j++)
					{
						blocks[j] = new BlockInfoContiguous(replication);
						blocks[j].ReadFields(@in);
					}
					string clientName = string.Empty;
					string clientMachine = string.Empty;
					bool underConstruction = false;
					FileDiffList fileDiffs = null;
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot, imgVersion))
					{
						// read diffs
						fileDiffs = SnapshotFSImageFormat.LoadFileDiffList(@in, this);
						if (isSnapshotINode)
						{
							underConstruction = @in.ReadBoolean();
							if (underConstruction)
							{
								clientName = FSImageSerialization.ReadString(@in);
								clientMachine = FSImageSerialization.ReadString(@in);
								// convert the last block to BlockUC
								if (blocks.Length > 0)
								{
									BlockInfoContiguous lastBlk = blocks[blocks.Length - 1];
									blocks[blocks.Length - 1] = new BlockInfoContiguousUnderConstruction(lastBlk, replication
										);
								}
							}
						}
					}
					PermissionStatus permissions = PermissionStatus.Read(@in);
					// return
					if (counter != null)
					{
						counter.Increment();
					}
					INodeFile file = new INodeFile(inodeId, localName, permissions, modificationTime, 
						atime, blocks, replication, blockSize, unchecked((byte)0));
					if (underConstruction)
					{
						file.ToUnderConstruction(clientName, clientMachine);
					}
					return fileDiffs == null ? file : new INodeFile(file, fileDiffs);
				}
				else
				{
					if (numBlocks == -1)
					{
						//directory
						//read quotas
						long nsQuota = @in.ReadLong();
						long dsQuota = -1L;
						if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.DiskspaceQuota, imgVersion
							))
						{
							dsQuota = @in.ReadLong();
						}
						//read snapshot info
						bool snapshottable = false;
						bool withSnapshot = false;
						if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot, imgVersion))
						{
							snapshottable = @in.ReadBoolean();
							if (!snapshottable)
							{
								withSnapshot = @in.ReadBoolean();
							}
						}
						PermissionStatus permissions = PermissionStatus.Read(@in);
						//return
						if (counter != null)
						{
							counter.Increment();
						}
						INodeDirectory dir = new INodeDirectory(inodeId, localName, permissions, modificationTime
							);
						if (nsQuota >= 0 || dsQuota >= 0)
						{
							dir.AddDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().NameSpaceQuota
								(nsQuota).StorageSpaceQuota(dsQuota).Build());
						}
						if (withSnapshot)
						{
							dir.AddSnapshotFeature(null);
						}
						if (snapshottable)
						{
							dir.AddSnapshottableFeature();
						}
						return dir;
					}
					else
					{
						if (numBlocks == -2)
						{
							//symlink
							if (!FileSystem.AreSymlinksEnabled())
							{
								throw new IOException("Symlinks not supported - please remove symlink before upgrading to this version of HDFS"
									);
							}
							string symlink = Text.ReadString(@in);
							PermissionStatus permissions = PermissionStatus.Read(@in);
							if (counter != null)
							{
								counter.Increment();
							}
							return new INodeSymlink(inodeId, localName, permissions, modificationTime, atime, 
								symlink);
						}
						else
						{
							if (numBlocks == -3)
							{
								//reference
								// Intentionally do not increment counter, because it is too difficult at
								// this point to assess whether or not this is a reference that counts
								// toward quota.
								bool isWithName = @in.ReadBoolean();
								// lastSnapshotId for WithName node, dstSnapshotId for DstReference node
								int snapshotId = @in.ReadInt();
								INodeReference.WithCount withCount = referenceMap.LoadINodeReferenceWithCount(isSnapshotINode
									, @in, this);
								if (isWithName)
								{
									return new INodeReference.WithName(null, withCount, localName, snapshotId);
								}
								else
								{
									INodeReference @ref = new INodeReference.DstReference(null, withCount, snapshotId
										);
									return @ref;
								}
							}
						}
					}
				}
				throw new IOException("Unknown inode type: numBlocks=" + numBlocks);
			}

			/// <summary>
			/// Load
			/// <see cref="INodeFileAttributes"/>
			/// .
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual INodeFileAttributes LoadINodeFileAttributes(DataInput @in)
			{
				int layoutVersion = GetLayoutVersion();
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.OptimizeSnapshotInodes, 
					layoutVersion))
				{
					return LoadINodeWithLocalName(true, @in, false).AsFile();
				}
				byte[] name = FSImageSerialization.ReadLocalName(@in);
				PermissionStatus permissions = PermissionStatus.Read(@in);
				long modificationTime = @in.ReadLong();
				long accessTime = @in.ReadLong();
				short replication = namesystem.GetBlockManager().AdjustReplication(@in.ReadShort(
					));
				long preferredBlockSize = @in.ReadLong();
				return new INodeFileAttributes.SnapshotCopy(name, permissions, null, modificationTime
					, accessTime, replication, preferredBlockSize, unchecked((byte)0), null);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual INodeDirectoryAttributes LoadINodeDirectoryAttributes(DataInput @in
				)
			{
				int layoutVersion = GetLayoutVersion();
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.OptimizeSnapshotInodes, 
					layoutVersion))
				{
					return LoadINodeWithLocalName(true, @in, false).AsDirectory();
				}
				byte[] name = FSImageSerialization.ReadLocalName(@in);
				PermissionStatus permissions = PermissionStatus.Read(@in);
				long modificationTime = @in.ReadLong();
				// Read quotas: quota by storage type does not need to be processed below.
				// It is handled only in protobuf based FsImagePBINode class for newer
				// fsImages. Tools using this class such as legacy-mode of offline image viewer
				// should only load legacy FSImages without newer features.
				long nsQuota = @in.ReadLong();
				long dsQuota = @in.ReadLong();
				return nsQuota == -1L && dsQuota == -1L ? new INodeDirectoryAttributes.SnapshotCopy
					(name, permissions, null, modificationTime, null) : new INodeDirectoryAttributes.CopyWithQuota
					(name, permissions, null, modificationTime, nsQuota, dsQuota, null, null);
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadFilesUnderConstruction(DataInput @in, bool supportSnapshot, StartupProgress.Counter
				 counter)
			{
				FSDirectory fsDir = namesystem.dir;
				int size = @in.ReadInt();
				Log.Info("Number of files under construction = " + size);
				for (int i = 0; i < size; i++)
				{
					INodeFile cons = FSImageSerialization.ReadINodeUnderConstruction(@in, namesystem, 
						GetLayoutVersion());
					counter.Increment();
					// verify that file exists in namespace
					string path = cons.GetLocalName();
					INodeFile oldnode = null;
					bool inSnapshot = false;
					if (path != null && FSDirectory.IsReservedName(path) && NameNodeLayoutVersion.Supports
						(LayoutVersion.Feature.AddInodeId, GetLayoutVersion()))
					{
						// TODO: for HDFS-5428, we use reserved path for those INodeFileUC in
						// snapshot. If we support INode ID in the layout version, we can use
						// the inode id to find the oldnode.
						oldnode = namesystem.dir.GetInode(cons.GetId()).AsFile();
						inSnapshot = true;
					}
					else
					{
						path = RenameReservedPathsOnUpgrade(path, GetLayoutVersion());
						INodesInPath iip = fsDir.GetINodesInPath(path, true);
						oldnode = INodeFile.ValueOf(iip.GetLastINode(), path);
					}
					FileUnderConstructionFeature uc = cons.GetFileUnderConstructionFeature();
					oldnode.ToUnderConstruction(uc.GetClientName(), uc.GetClientMachine());
					if (oldnode.NumBlocks() > 0)
					{
						BlockInfoContiguous ucBlock = cons.GetLastBlock();
						// we do not replace the inode, just replace the last block of oldnode
						BlockInfoContiguous info = namesystem.GetBlockManager().AddBlockCollection(ucBlock
							, oldnode);
						oldnode.SetBlock(oldnode.NumBlocks() - 1, info);
					}
					if (!inSnapshot)
					{
						namesystem.leaseManager.AddLease(cons.GetFileUnderConstructionFeature().GetClientName
							(), path);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadSecretManagerState(DataInput @in)
			{
				int imgVersion = GetLayoutVersion();
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.DelegationToken, imgVersion
					))
				{
					//SecretManagerState is not available.
					//This must not happen if security is turned on.
					return;
				}
				namesystem.LoadSecretManagerStateCompat(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadCacheManagerState(DataInput @in)
			{
				int imgVersion = GetLayoutVersion();
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Caching, imgVersion))
				{
					return;
				}
				namesystem.GetCacheManager().LoadStateCompat(@in);
			}

			private int GetLayoutVersion()
			{
				return namesystem.GetFSImage().GetStorage().GetLayoutVersion();
			}

			private bool IsRoot(byte[][] path)
			{
				return path.Length == 1 && path[0] == null;
			}

			private bool IsParent(byte[][] path, byte[][] parent)
			{
				if (path == null || parent == null)
				{
					return false;
				}
				if (parent.Length == 0 || path.Length != parent.Length + 1)
				{
					return false;
				}
				bool isParent = true;
				for (int i = 0; i < parent.Length; i++)
				{
					isParent = isParent && Arrays.Equals(path[i], parent[i]);
				}
				return isParent;
			}

			/// <summary>Return string representing the parent of the given path.</summary>
			internal virtual string GetParent(string path)
			{
				return Sharpen.Runtime.Substring(path, 0, path.LastIndexOf(Path.Separator));
			}

			internal virtual byte[][] GetParent(byte[][] path)
			{
				byte[][] result = new byte[path.Length - 1][];
				for (int i = 0; i < result.Length; i++)
				{
					result[i] = new byte[path[i].Length];
					System.Array.Copy(path[i], 0, result[i], 0, path[i].Length);
				}
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetSnapshot
				(DataInput @in)
			{
				return snapshotMap[@in.ReadInt()];
			}
		}

		[VisibleForTesting]
		public static readonly SortedDictionary<string, string> renameReservedMap = new SortedDictionary
			<string, string>();

		/// <summary>
		/// Use the default key-value pairs that will be used to determine how to
		/// rename reserved paths on upgrade.
		/// </summary>
		[VisibleForTesting]
		public static void UseDefaultRenameReservedPairs()
		{
			renameReservedMap.Clear();
			foreach (string key in HdfsConstants.ReservedPathComponents)
			{
				renameReservedMap[key] = key + "." + HdfsConstants.NamenodeLayoutVersion + "." + 
					"UPGRADE_RENAMED";
			}
		}

		/// <summary>
		/// Set the key-value pairs that will be used to determine how to rename
		/// reserved paths on upgrade.
		/// </summary>
		[VisibleForTesting]
		public static void SetRenameReservedPairs(string renameReserved)
		{
			// Clear and set the default values
			UseDefaultRenameReservedPairs();
			// Overwrite with provided values
			SetRenameReservedMapInternal(renameReserved);
		}

		private static void SetRenameReservedMapInternal(string renameReserved)
		{
			ICollection<string> pairs = StringUtils.GetTrimmedStringCollection(renameReserved
				);
			foreach (string p in pairs)
			{
				string[] pair = StringUtils.Split(p, '/', '=');
				Preconditions.CheckArgument(pair.Length == 2, "Could not parse key-value pair " +
					 p);
				string key = pair[0];
				string value = pair[1];
				Preconditions.CheckArgument(DFSUtil.IsReservedPathComponent(key), "Unknown reserved path "
					 + key);
				Preconditions.CheckArgument(DFSUtil.IsValidNameForComponent(value), "Invalid rename path for "
					 + key + ": " + value);
				Log.Info("Will rename reserved path " + key + " to " + value);
				renameReservedMap[key] = value;
			}
		}

		/// <summary>
		/// When upgrading from an old version, the filesystem could contain paths
		/// that are now reserved in the new version (e.g.
		/// </summary>
		/// <remarks>
		/// When upgrading from an old version, the filesystem could contain paths
		/// that are now reserved in the new version (e.g. .snapshot). This renames
		/// these new reserved paths to a user-specified value to avoid collisions
		/// with the reserved name.
		/// </remarks>
		/// <param name="path">Old path potentially containing a reserved path</param>
		/// <returns>New path with reserved path components renamed to user value</returns>
		internal static string RenameReservedPathsOnUpgrade(string path, int layoutVersion
			)
		{
			string oldPath = path;
			// If any known LVs aren't supported, we're doing an upgrade
			if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, layoutVersion
				))
			{
				string[] components = INode.GetPathNames(path);
				// Only need to worry about the root directory
				if (components.Length > 1)
				{
					components[1] = DFSUtil.Bytes2String(RenameReservedRootComponentOnUpgrade(DFSUtil
						.String2Bytes(components[1]), layoutVersion));
					path = DFSUtil.Strings2PathString(components);
				}
			}
			if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot, layoutVersion
				))
			{
				string[] components = INode.GetPathNames(path);
				// Special case the root path
				if (components.Length == 0)
				{
					return path;
				}
				for (int i = 0; i < components.Length; i++)
				{
					components[i] = DFSUtil.Bytes2String(RenameReservedComponentOnUpgrade(DFSUtil.String2Bytes
						(components[i]), layoutVersion));
				}
				path = DFSUtil.Strings2PathString(components);
			}
			if (!path.Equals(oldPath))
			{
				Log.Info("Upgrade process renamed reserved path " + oldPath + " to " + path);
			}
			return path;
		}

		private static readonly string ReservedErrorMsg = FSDirectory.DotReservedPathPrefix
			 + " is a reserved path and " + HdfsConstants.DotSnapshotDir + " is a reserved path component in"
			 + " this version of HDFS. Please rollback and delete or rename" + " this path, or upgrade with the "
			 + HdfsServerConstants.StartupOption.Renamereserved.GetName() + " [key-value pairs]"
			 + " option to automatically rename these paths during upgrade.";

		/// <summary>
		/// Same as
		/// <see cref="RenameReservedPathsOnUpgrade(string, int)"/>
		/// , but for a single
		/// byte array path component.
		/// </summary>
		private static byte[] RenameReservedComponentOnUpgrade(byte[] component, int layoutVersion
			)
		{
			// If the LV doesn't support snapshots, we're doing an upgrade
			if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot, layoutVersion
				))
			{
				if (Arrays.Equals(component, HdfsConstants.DotSnapshotDirBytes))
				{
					Preconditions.CheckArgument(renameReservedMap.Contains(HdfsConstants.DotSnapshotDir
						), ReservedErrorMsg);
					component = DFSUtil.String2Bytes(renameReservedMap[HdfsConstants.DotSnapshotDir]);
				}
			}
			return component;
		}

		/// <summary>
		/// Same as
		/// <see cref="RenameReservedPathsOnUpgrade(string, int)"/>
		/// , but for a single
		/// byte array path component.
		/// </summary>
		private static byte[] RenameReservedRootComponentOnUpgrade(byte[] component, int 
			layoutVersion)
		{
			// If the LV doesn't support inode IDs, we're doing an upgrade
			if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, layoutVersion
				))
			{
				if (Arrays.Equals(component, FSDirectory.DotReserved))
				{
					Preconditions.CheckArgument(renameReservedMap.Contains(FSDirectory.DotReservedString
						), ReservedErrorMsg);
					string renameString = renameReservedMap[FSDirectory.DotReservedString];
					component = DFSUtil.String2Bytes(renameString);
					Log.Info("Renamed root path " + FSDirectory.DotReservedString + " to " + renameString
						);
				}
			}
			return component;
		}

		/// <summary>A one-shot class responsible for writing an image file.</summary>
		/// <remarks>
		/// A one-shot class responsible for writing an image file.
		/// The write() function should be called once, after which the getter
		/// functions may be used to retrieve information about the file that was written.
		/// This is replaced by the PB-based FSImage. The class is to maintain
		/// compatibility for the external fsimage tool.
		/// </remarks>
		internal class Saver
		{
			private const int LayoutVersion = -51;

			public const int CheckCancelInterval = 4096;

			private readonly SaveNamespaceContext context;

			/// <summary>Set to true once an image has been written</summary>
			private bool saved = false;

			private long checkCancelCounter = 0;

			/// <summary>The MD5 checksum of the file that was written</summary>
			private MD5Hash savedDigest;

			private readonly SnapshotFSImageFormat.ReferenceMap referenceMap = new SnapshotFSImageFormat.ReferenceMap
				();

			private readonly IDictionary<long, INodeFile> snapshotUCMap = new Dictionary<long
				, INodeFile>();

			/// <exception cref="System.InvalidOperationException">if the instance has not yet saved an image
			/// 	</exception>
			private void CheckSaved()
			{
				if (!saved)
				{
					throw new InvalidOperationException("FSImageSaver has not saved an image");
				}
			}

			/// <exception cref="System.InvalidOperationException">if the instance has already saved an image
			/// 	</exception>
			private void CheckNotSaved()
			{
				if (saved)
				{
					throw new InvalidOperationException("FSImageSaver has already saved an image");
				}
			}

			internal Saver(SaveNamespaceContext context)
			{
				this.context = context;
			}

			/// <summary>Return the MD5 checksum of the image file that was saved.</summary>
			internal virtual MD5Hash GetSavedDigest()
			{
				CheckSaved();
				return savedDigest;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Save(FilePath newFile, FSImageCompression compression)
			{
				CheckNotSaved();
				FSNamesystem sourceNamesystem = context.GetSourceNamesystem();
				INodeDirectory rootDir = sourceNamesystem.dir.rootDir;
				long numINodes = rootDir.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetNameSpace
					();
				string sdPath = newFile.GetParentFile().GetParentFile().GetAbsolutePath();
				Step step = new Step(StepType.Inodes, sdPath);
				StartupProgress prog = NameNode.GetStartupProgress();
				prog.BeginStep(Phase.SavingCheckpoint, step);
				prog.SetTotal(Phase.SavingCheckpoint, step, numINodes);
				StartupProgress.Counter counter = prog.GetCounter(Phase.SavingCheckpoint, step);
				long startTime = Time.MonotonicNow();
				//
				// Write out data
				//
				MessageDigest digester = MD5Hash.GetDigester();
				FileOutputStream fout = new FileOutputStream(newFile);
				DigestOutputStream fos = new DigestOutputStream(fout, digester);
				DataOutputStream @out = new DataOutputStream(fos);
				try
				{
					@out.WriteInt(LayoutVersion);
					LayoutFlags.Write(@out);
					// We use the non-locked version of getNamespaceInfo here since
					// the coordinating thread of saveNamespace already has read-locked
					// the namespace for us. If we attempt to take another readlock
					// from the actual saver thread, there's a potential of a
					// fairness-related deadlock. See the comments on HDFS-2223.
					@out.WriteInt(sourceNamesystem.UnprotectedGetNamespaceInfo().GetNamespaceID());
					@out.WriteLong(numINodes);
					@out.WriteLong(sourceNamesystem.GetBlockIdManager().GetGenerationStampV1());
					@out.WriteLong(sourceNamesystem.GetBlockIdManager().GetGenerationStampV2());
					@out.WriteLong(sourceNamesystem.GetBlockIdManager().GetGenerationStampAtblockIdSwitch
						());
					@out.WriteLong(sourceNamesystem.GetBlockIdManager().GetLastAllocatedBlockId());
					@out.WriteLong(context.GetTxId());
					@out.WriteLong(sourceNamesystem.dir.GetLastInodeId());
					sourceNamesystem.GetSnapshotManager().Write(@out);
					// write compression info and set up compressed stream
					@out = compression.WriteHeaderAndWrapStream(fos);
					Log.Info("Saving image file " + newFile + " using " + compression);
					// save the root
					SaveINode2Image(rootDir, @out, false, referenceMap, counter);
					// save the rest of the nodes
					SaveImage(rootDir, @out, true, false, counter);
					prog.EndStep(Phase.SavingCheckpoint, step);
					// Now that the step is finished, set counter equal to total to adjust
					// for possible under-counting due to reference inodes.
					prog.SetCount(Phase.SavingCheckpoint, step, numINodes);
					// save files under construction
					// TODO: for HDFS-5428, since we cannot break the compatibility of
					// fsimage, we store part of the under-construction files that are only
					// in snapshots in this "under-construction-file" section. As a
					// temporary solution, we use "/.reserved/.inodes/<inodeid>" as their
					// paths, so that when loading fsimage we do not put them into the lease
					// map. In the future, we can remove this hack when we can bump the
					// layout version.
					sourceNamesystem.SaveFilesUnderConstruction(@out, snapshotUCMap);
					context.CheckCancelled();
					sourceNamesystem.SaveSecretManagerStateCompat(@out, sdPath);
					context.CheckCancelled();
					sourceNamesystem.GetCacheManager().SaveStateCompat(@out, sdPath);
					context.CheckCancelled();
					@out.Flush();
					context.CheckCancelled();
					fout.GetChannel().Force(true);
				}
				finally
				{
					@out.Close();
				}
				saved = true;
				// set md5 of the saved image
				savedDigest = new MD5Hash(digester.Digest());
				Log.Info("Image file " + newFile + " of size " + newFile.Length() + " bytes saved in "
					 + (Time.MonotonicNow() - startTime) / 1000 + " seconds.");
			}

			/// <summary>Save children INodes.</summary>
			/// <param name="children">The list of children INodes</param>
			/// <param name="out">The DataOutputStream to write</param>
			/// <param name="inSnapshot">
			/// Whether the parent directory or its ancestor is in
			/// the deleted list of some snapshot (caused by rename or
			/// deletion)
			/// </param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <returns>Number of children that are directory</returns>
			/// <exception cref="System.IO.IOException"/>
			private int SaveChildren(ReadOnlyList<INode> children, DataOutputStream @out, bool
				 inSnapshot, StartupProgress.Counter counter)
			{
				// Write normal children INode.
				@out.WriteInt(children.Size());
				int dirNum = 0;
				foreach (INode child in children)
				{
					// print all children first
					// TODO: for HDFS-5428, we cannot change the format/content of fsimage
					// here, thus even if the parent directory is in snapshot, we still
					// do not handle INodeUC as those stored in deleted list
					SaveINode2Image(child, @out, false, referenceMap, counter);
					if (child.IsDirectory())
					{
						dirNum++;
					}
					else
					{
						if (inSnapshot && child.IsFile() && child.AsFile().IsUnderConstruction())
						{
							this.snapshotUCMap[child.GetId()] = child.AsFile();
						}
					}
					if (checkCancelCounter++ % CheckCancelInterval == 0)
					{
						context.CheckCancelled();
					}
				}
				return dirNum;
			}

			/// <summary>Save file tree image starting from the given root.</summary>
			/// <remarks>
			/// Save file tree image starting from the given root.
			/// This is a recursive procedure, which first saves all children and
			/// snapshot diffs of a current directory and then moves inside the
			/// sub-directories.
			/// </remarks>
			/// <param name="current">The current node</param>
			/// <param name="out">The DataoutputStream to write the image</param>
			/// <param name="toSaveSubtree">
			/// Whether or not to save the subtree to fsimage. For
			/// reference node, its subtree may already have been
			/// saved before.
			/// </param>
			/// <param name="inSnapshot">Whether the current directory is in snapshot</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException"/>
			private void SaveImage(INodeDirectory current, DataOutputStream @out, bool toSaveSubtree
				, bool inSnapshot, StartupProgress.Counter counter)
			{
				// write the inode id of the directory
				@out.WriteLong(current.GetId());
				if (!toSaveSubtree)
				{
					return;
				}
				ReadOnlyList<INode> children = current.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
				int dirNum = 0;
				IList<INodeDirectory> snapshotDirs = null;
				DirectoryWithSnapshotFeature sf = current.GetDirectoryWithSnapshotFeature();
				if (sf != null)
				{
					snapshotDirs = new AList<INodeDirectory>();
					sf.GetSnapshotDirectory(snapshotDirs);
					dirNum += snapshotDirs.Count;
				}
				// 2. Write INodeDirectorySnapshottable#snapshotsByNames to record all
				// Snapshots
				if (current.IsDirectory() && current.AsDirectory().IsSnapshottable())
				{
					SnapshotFSImageFormat.SaveSnapshots(current.AsDirectory(), @out);
				}
				else
				{
					@out.WriteInt(-1);
				}
				// # of snapshots
				// 3. Write children INode
				dirNum += SaveChildren(children, @out, inSnapshot, counter);
				// 4. Write DirectoryDiff lists, if there is any.
				SnapshotFSImageFormat.SaveDirectoryDiffList(current, @out, referenceMap);
				// Write sub-tree of sub-directories, including possible snapshots of
				// deleted sub-directories
				@out.WriteInt(dirNum);
				// the number of sub-directories
				foreach (INode child in children)
				{
					if (!child.IsDirectory())
					{
						continue;
					}
					// make sure we only save the subtree under a reference node once
					bool toSave = child.IsReference() ? referenceMap.ToProcessSubtree(child.GetId()) : 
						true;
					SaveImage(child.AsDirectory(), @out, toSave, inSnapshot, counter);
				}
				if (snapshotDirs != null)
				{
					foreach (INodeDirectory subDir in snapshotDirs)
					{
						// make sure we only save the subtree under a reference node once
						bool toSave = subDir.GetParentReference() != null ? referenceMap.ToProcessSubtree
							(subDir.GetId()) : true;
						SaveImage(subDir, @out, toSave, true, counter);
					}
				}
			}

			/// <summary>Saves inode and increments progress counter.</summary>
			/// <param name="inode">INode to save</param>
			/// <param name="out">DataOutputStream to receive inode</param>
			/// <param name="writeUnderConstruction">boolean true if this is under construction</param>
			/// <param name="referenceMap">ReferenceMap containing reference inodes</param>
			/// <param name="counter">Counter to increment for namenode startup progress</param>
			/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
			private void SaveINode2Image(INode inode, DataOutputStream @out, bool writeUnderConstruction
				, SnapshotFSImageFormat.ReferenceMap referenceMap, StartupProgress.Counter counter
				)
			{
				FSImageSerialization.SaveINode2Image(inode, @out, writeUnderConstruction, referenceMap
					);
				// Intentionally do not increment counter for reference inodes, because it
				// is too difficult at this point to assess whether or not this is a
				// reference that counts toward quota.
				if (!(inode is INodeReference))
				{
					counter.Increment();
				}
			}
		}
	}
}

using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// ImageLoaderCurrent processes Hadoop FSImage files and walks over
	/// them using a provided ImageVisitor, calling the visitor at each element
	/// enumerated below.
	/// </summary>
	/// <remarks>
	/// ImageLoaderCurrent processes Hadoop FSImage files and walks over
	/// them using a provided ImageVisitor, calling the visitor at each element
	/// enumerated below.
	/// The only difference between v18 and v19 was the utilization of the
	/// stickybit.  Therefore, the same viewer can reader either format.
	/// Versions -19 fsimage layout (with changes from -16 up):
	/// Image version (int)
	/// Namepsace ID (int)
	/// NumFiles (long)
	/// Generation stamp (long)
	/// INodes (count = NumFiles)
	/// INode
	/// Path (String)
	/// Replication (short)
	/// Modification Time (long as date)
	/// Access Time (long) // added in -16
	/// Block size (long)
	/// Num blocks (int)
	/// Blocks (count = Num blocks)
	/// Block
	/// Block ID (long)
	/// Num bytes (long)
	/// Generation stamp (long)
	/// Namespace Quota (long)
	/// Diskspace Quota (long) // added in -18
	/// Permissions
	/// Username (String)
	/// Groupname (String)
	/// OctalPerms (short -&gt; String)  // Modified in -19
	/// Symlink (String) // added in -23
	/// NumINodesUnderConstruction (int)
	/// INodesUnderConstruction (count = NumINodesUnderConstruction)
	/// INodeUnderConstruction
	/// Path (bytes as string)
	/// Replication (short)
	/// Modification time (long as date)
	/// Preferred block size (long)
	/// Num blocks (int)
	/// Blocks
	/// Block
	/// Block ID (long)
	/// Num bytes (long)
	/// Generation stamp (long)
	/// Permissions
	/// Username (String)
	/// Groupname (String)
	/// OctalPerms (short -&gt; String)
	/// Client Name (String)
	/// Client Machine (String)
	/// NumLocations (int)
	/// DatanodeDescriptors (count = numLocations) // not loaded into memory
	/// short                                    // but still in file
	/// long
	/// string
	/// long
	/// int
	/// string
	/// string
	/// enum
	/// CurrentDelegationKeyId (int)
	/// NumDelegationKeys (int)
	/// DelegationKeys (count = NumDelegationKeys)
	/// DelegationKeyLength (vint)
	/// DelegationKey (bytes)
	/// DelegationTokenSequenceNumber (int)
	/// NumDelegationTokens (int)
	/// DelegationTokens (count = NumDelegationTokens)
	/// DelegationTokenIdentifier
	/// owner (String)
	/// renewer (String)
	/// realUser (String)
	/// issueDate (vlong)
	/// maxDate (vlong)
	/// sequenceNumber (vint)
	/// masterKeyId (vint)
	/// expiryTime (long)
	/// </remarks>
	internal class ImageLoaderCurrent : ImageLoader
	{
		protected internal readonly DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm"
			);

		private static int[] versions = new int[] { -16, -17, -18, -19, -20, -21, -22, -23
			, -24, -25, -26, -27, -28, -30, -31, -32, -33, -34, -35, -36, -37, -38, -39, -40
			, -41, -42, -43, -44, -45, -46, -47, -48, -49, -50, -51 };

		private int imageVersion = 0;

		private readonly IDictionary<long, bool> subtreeMap = new Dictionary<long, bool>(
			);

		private readonly IDictionary<long, string> dirNodeMap = new Dictionary<long, string
			>();

		/* (non-Javadoc)
		* @see ImageLoader#canProcessVersion(int)
		*/
		public override bool CanLoadVersion(int version)
		{
			foreach (int v in versions)
			{
				if (v == version)
				{
					return true;
				}
			}
			return false;
		}

		/* (non-Javadoc)
		* @see ImageLoader#processImage(java.io.DataInputStream, ImageVisitor, boolean)
		*/
		/// <exception cref="System.IO.IOException"/>
		public override void LoadImage(DataInputStream @in, ImageVisitor v, bool skipBlocks
			)
		{
			bool done = false;
			try
			{
				v.Start();
				v.VisitEnclosingElement(ImageVisitor.ImageElement.FsImage);
				imageVersion = @in.ReadInt();
				if (!CanLoadVersion(imageVersion))
				{
					throw new IOException("Cannot process fslayout version " + imageVersion);
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddLayoutFlags, imageVersion
					))
				{
					LayoutFlags.Read(@in);
				}
				v.Visit(ImageVisitor.ImageElement.ImageVersion, imageVersion);
				v.Visit(ImageVisitor.ImageElement.NamespaceId, @in.ReadInt());
				long numInodes = @in.ReadLong();
				v.Visit(ImageVisitor.ImageElement.GenerationStamp, @in.ReadLong());
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.SequentialBlockId, imageVersion
					))
				{
					v.Visit(ImageVisitor.ImageElement.GenerationStampV2, @in.ReadLong());
					v.Visit(ImageVisitor.ImageElement.GenerationStampV1Limit, @in.ReadLong());
					v.Visit(ImageVisitor.ImageElement.LastAllocatedBlockId, @in.ReadLong());
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.StoredTxids, imageVersion
					))
				{
					v.Visit(ImageVisitor.ImageElement.TransactionId, @in.ReadLong());
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, imageVersion
					))
				{
					v.Visit(ImageVisitor.ImageElement.LastInodeId, @in.ReadLong());
				}
				bool supportSnapshot = NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot
					, imageVersion);
				if (supportSnapshot)
				{
					v.Visit(ImageVisitor.ImageElement.SnapshotCounter, @in.ReadInt());
					int numSnapshots = @in.ReadInt();
					v.Visit(ImageVisitor.ImageElement.NumSnapshotsTotal, numSnapshots);
					for (int i = 0; i < numSnapshots; i++)
					{
						ProcessSnapshot(@in, v);
					}
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FsimageCompression, imageVersion
					))
				{
					bool isCompressed = @in.ReadBoolean();
					v.Visit(ImageVisitor.ImageElement.IsCompressed, isCompressed.ToString());
					if (isCompressed)
					{
						string codecClassName = Text.ReadString(@in);
						v.Visit(ImageVisitor.ImageElement.CompressCodec, codecClassName);
						CompressionCodecFactory codecFac = new CompressionCodecFactory(new Configuration(
							));
						CompressionCodec codec = codecFac.GetCodecByClassName(codecClassName);
						if (codec == null)
						{
							throw new IOException("Image compression codec not supported: " + codecClassName);
						}
						@in = new DataInputStream(codec.CreateInputStream(@in));
					}
				}
				ProcessINodes(@in, v, numInodes, skipBlocks, supportSnapshot);
				subtreeMap.Clear();
				dirNodeMap.Clear();
				ProcessINodesUC(@in, v, skipBlocks);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.DelegationToken, imageVersion
					))
				{
					ProcessDelegationTokens(@in, v);
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Caching, imageVersion))
				{
					ProcessCacheManagerState(@in, v);
				}
				v.LeaveEnclosingElement();
				// FSImage
				done = true;
			}
			finally
			{
				if (done)
				{
					v.Finish();
				}
				else
				{
					v.FinishAbnormally();
				}
			}
		}

		/// <summary>Process CacheManager state from the fsimage.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessCacheManagerState(DataInputStream @in, ImageVisitor v)
		{
			v.Visit(ImageVisitor.ImageElement.CacheNextEntryId, @in.ReadLong());
			int numPools = @in.ReadInt();
			for (int i = 0; i < numPools; i++)
			{
				v.Visit(ImageVisitor.ImageElement.CachePoolName, Text.ReadString(@in));
				ProcessCachePoolPermission(@in, v);
				v.Visit(ImageVisitor.ImageElement.CachePoolWeight, @in.ReadInt());
			}
			int numEntries = @in.ReadInt();
			for (int i_1 = 0; i_1 < numEntries; i_1++)
			{
				v.Visit(ImageVisitor.ImageElement.CacheEntryPath, Text.ReadString(@in));
				v.Visit(ImageVisitor.ImageElement.CacheEntryReplication, @in.ReadShort());
				v.Visit(ImageVisitor.ImageElement.CacheEntryPoolName, Text.ReadString(@in));
			}
		}

		/// <summary>Process the Delegation Token related section in fsimage.</summary>
		/// <param name="in">DataInputStream to process</param>
		/// <param name="v">Visitor to walk over records</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessDelegationTokens(DataInputStream @in, ImageVisitor v)
		{
			v.Visit(ImageVisitor.ImageElement.CurrentDelegationKeyId, @in.ReadInt());
			int numDKeys = @in.ReadInt();
			v.VisitEnclosingElement(ImageVisitor.ImageElement.DelegationKeys, ImageVisitor.ImageElement
				.NumDelegationKeys, numDKeys);
			for (int i = 0; i < numDKeys; i++)
			{
				DelegationKey key = new DelegationKey();
				key.ReadFields(@in);
				v.Visit(ImageVisitor.ImageElement.DelegationKey, key.ToString());
			}
			v.LeaveEnclosingElement();
			v.Visit(ImageVisitor.ImageElement.DelegationTokenSequenceNumber, @in.ReadInt());
			int numDTokens = @in.ReadInt();
			v.VisitEnclosingElement(ImageVisitor.ImageElement.DelegationTokens, ImageVisitor.ImageElement
				.NumDelegationTokens, numDTokens);
			for (int i_1 = 0; i_1 < numDTokens; i_1++)
			{
				DelegationTokenIdentifier id = new DelegationTokenIdentifier();
				id.ReadFields(@in);
				long expiryTime = @in.ReadLong();
				v.VisitEnclosingElement(ImageVisitor.ImageElement.DelegationTokenIdentifier);
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierKind, id.GetKind().ToString
					());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierSeqno, id.GetSequenceNumber
					());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierOwner, id.GetOwner().ToString
					());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierRenewer, id.GetRenewer
					().ToString());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierRealuser, id.GetRealUser
					().ToString());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierIssueDate, id.GetIssueDate
					());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierMaxDate, id.GetMaxDate
					());
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierExpiryTime, expiryTime
					);
				v.Visit(ImageVisitor.ImageElement.DelegationTokenIdentifierMasterKeyId, id.GetMasterKeyId
					());
				v.LeaveEnclosingElement();
			}
			// DELEGATION_TOKEN_IDENTIFIER
			v.LeaveEnclosingElement();
		}

		// DELEGATION_TOKENS
		/// <summary>Process the INodes under construction section of the fsimage.</summary>
		/// <param name="in">DataInputStream to process</param>
		/// <param name="v">Visitor to walk over inodes</param>
		/// <param name="skipBlocks">Walk over each block?</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessINodesUC(DataInputStream @in, ImageVisitor v, bool skipBlocks
			)
		{
			int numINUC = @in.ReadInt();
			v.VisitEnclosingElement(ImageVisitor.ImageElement.InodesUnderConstruction, ImageVisitor.ImageElement
				.NumInodesUnderConstruction, numINUC);
			for (int i = 0; i < numINUC; i++)
			{
				v.VisitEnclosingElement(ImageVisitor.ImageElement.InodeUnderConstruction);
				byte[] name = FSImageSerialization.ReadBytes(@in);
				string n = Sharpen.Runtime.GetStringForBytes(name, "UTF8");
				v.Visit(ImageVisitor.ImageElement.InodePath, n);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, imageVersion
					))
				{
					long inodeId = @in.ReadLong();
					v.Visit(ImageVisitor.ImageElement.InodeId, inodeId);
				}
				v.Visit(ImageVisitor.ImageElement.Replication, @in.ReadShort());
				v.Visit(ImageVisitor.ImageElement.ModificationTime, FormatDate(@in.ReadLong()));
				v.Visit(ImageVisitor.ImageElement.PreferredBlockSize, @in.ReadLong());
				int numBlocks = @in.ReadInt();
				ProcessBlocks(@in, v, numBlocks, skipBlocks);
				ProcessPermission(@in, v);
				v.Visit(ImageVisitor.ImageElement.ClientName, FSImageSerialization.ReadString(@in
					));
				v.Visit(ImageVisitor.ImageElement.ClientMachine, FSImageSerialization.ReadString(
					@in));
				// Skip over the datanode descriptors, which are still stored in the
				// file but are not used by the datanode or loaded into memory
				int numLocs = @in.ReadInt();
				for (int j = 0; j < numLocs; j++)
				{
					@in.ReadShort();
					@in.ReadLong();
					@in.ReadLong();
					@in.ReadLong();
					@in.ReadInt();
					FSImageSerialization.ReadString(@in);
					FSImageSerialization.ReadString(@in);
					WritableUtils.ReadEnum<DatanodeInfo.AdminStates>(@in);
				}
				v.LeaveEnclosingElement();
			}
			// INodeUnderConstruction
			v.LeaveEnclosingElement();
		}

		// INodesUnderConstruction
		/// <summary>Process the blocks section of the fsimage.</summary>
		/// <param name="in">Datastream to process</param>
		/// <param name="v">Visitor to walk over inodes</param>
		/// <param name="skipBlocks">Walk over each block?</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessBlocks(DataInputStream @in, ImageVisitor v, int numBlocks, bool
			 skipBlocks)
		{
			v.VisitEnclosingElement(ImageVisitor.ImageElement.Blocks, ImageVisitor.ImageElement
				.NumBlocks, numBlocks);
			// directory or symlink or reference node, no blocks to process    
			if (numBlocks < 0)
			{
				v.LeaveEnclosingElement();
				// Blocks
				return;
			}
			if (skipBlocks)
			{
				int bytesToSkip = ((long.Size * 3) / 8) * numBlocks;
				/* fields */
				/*bits*/
				if (@in.SkipBytes(bytesToSkip) != bytesToSkip)
				{
					throw new IOException("Error skipping over blocks");
				}
			}
			else
			{
				for (int j = 0; j < numBlocks; j++)
				{
					v.VisitEnclosingElement(ImageVisitor.ImageElement.Block);
					v.Visit(ImageVisitor.ImageElement.BlockId, @in.ReadLong());
					v.Visit(ImageVisitor.ImageElement.NumBytes, @in.ReadLong());
					v.Visit(ImageVisitor.ImageElement.GenerationStamp, @in.ReadLong());
					v.LeaveEnclosingElement();
				}
			}
			// Block
			v.LeaveEnclosingElement();
		}

		// Blocks
		/// <summary>Extract the INode permissions stored in the fsimage file.</summary>
		/// <param name="in">Datastream to process</param>
		/// <param name="v">Visitor to walk over inodes</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessPermission(DataInputStream @in, ImageVisitor v)
		{
			v.VisitEnclosingElement(ImageVisitor.ImageElement.Permissions);
			v.Visit(ImageVisitor.ImageElement.UserName, Text.ReadString(@in));
			v.Visit(ImageVisitor.ImageElement.GroupName, Text.ReadString(@in));
			FsPermission fsp = new FsPermission(@in.ReadShort());
			v.Visit(ImageVisitor.ImageElement.PermissionString, fsp.ToString());
			v.LeaveEnclosingElement();
		}

		// Permissions
		/// <summary>Extract CachePool permissions stored in the fsimage file.</summary>
		/// <param name="in">Datastream to process</param>
		/// <param name="v">Visitor to walk over inodes</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessCachePoolPermission(DataInputStream @in, ImageVisitor v)
		{
			v.VisitEnclosingElement(ImageVisitor.ImageElement.Permissions);
			v.Visit(ImageVisitor.ImageElement.CachePoolOwnerName, Text.ReadString(@in));
			v.Visit(ImageVisitor.ImageElement.CachePoolGroupName, Text.ReadString(@in));
			FsPermission fsp = new FsPermission(@in.ReadShort());
			v.Visit(ImageVisitor.ImageElement.CachePoolPermissionString, fsp.ToString());
			v.LeaveEnclosingElement();
		}

		// Permissions
		/// <summary>Process the INode records stored in the fsimage.</summary>
		/// <param name="in">Datastream to process</param>
		/// <param name="v">Visitor to walk over INodes</param>
		/// <param name="numInodes">Number of INodes stored in file</param>
		/// <param name="skipBlocks">Process all the blocks within the INode?</param>
		/// <param name="supportSnapshot">Whether or not the imageVersion supports snapshot</param>
		/// <exception cref="VisitException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessINodes(DataInputStream @in, ImageVisitor v, long numInodes, bool
			 skipBlocks, bool supportSnapshot)
		{
			v.VisitEnclosingElement(ImageVisitor.ImageElement.Inodes, ImageVisitor.ImageElement
				.NumInodes, numInodes);
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FsimageNameOptimization, 
				imageVersion))
			{
				if (!supportSnapshot)
				{
					ProcessLocalNameINodes(@in, v, numInodes, skipBlocks);
				}
				else
				{
					ProcessLocalNameINodesWithSnapshot(@in, v, skipBlocks);
				}
			}
			else
			{
				// full path name
				ProcessFullNameINodes(@in, v, numInodes, skipBlocks);
			}
			v.LeaveEnclosingElement();
		}

		// INodes
		/// <summary>Process image with full path name</summary>
		/// <param name="in">image stream</param>
		/// <param name="v">visitor</param>
		/// <param name="numInodes">number of indoes to read</param>
		/// <param name="skipBlocks">skip blocks or not</param>
		/// <exception cref="System.IO.IOException">if there is any error occurs</exception>
		private void ProcessLocalNameINodes(DataInputStream @in, ImageVisitor v, long numInodes
			, bool skipBlocks)
		{
			// process root
			ProcessINode(@in, v, skipBlocks, string.Empty, false);
			numInodes--;
			while (numInodes > 0)
			{
				numInodes -= ProcessDirectory(@in, v, skipBlocks);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ProcessDirectory(DataInputStream @in, ImageVisitor v, bool skipBlocks
			)
		{
			string parentName = FSImageSerialization.ReadString(@in);
			return ProcessChildren(@in, v, skipBlocks, parentName);
		}

		/// <summary>Process image with local path name and snapshot support</summary>
		/// <param name="in">image stream</param>
		/// <param name="v">visitor</param>
		/// <param name="skipBlocks">skip blocks or not</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessLocalNameINodesWithSnapshot(DataInputStream @in, ImageVisitor
			 v, bool skipBlocks)
		{
			// process root
			ProcessINode(@in, v, skipBlocks, string.Empty, false);
			ProcessDirectoryWithSnapshot(@in, v, skipBlocks);
		}

		/// <summary>Process directories when snapshot is supported.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessDirectoryWithSnapshot(DataInputStream @in, ImageVisitor v, bool
			 skipBlocks)
		{
			// 1. load dir node id
			long inodeId = @in.ReadLong();
			string dirName = Sharpen.Collections.Remove(dirNodeMap, inodeId);
			bool visitedRef = subtreeMap[inodeId];
			if (visitedRef != null)
			{
				if (visitedRef)
				{
					// the subtree has been visited
					return;
				}
				else
				{
					// first time to visit
					subtreeMap[inodeId] = true;
				}
			}
			// else the dir is not linked by a RefNode, thus cannot be revisited
			// 2. load possible snapshots
			ProcessSnapshots(@in, v, dirName);
			// 3. load children nodes
			ProcessChildren(@in, v, skipBlocks, dirName);
			// 4. load possible directory diff list
			ProcessDirectoryDiffList(@in, v, dirName);
			// recursively process sub-directories
			int numSubTree = @in.ReadInt();
			for (int i = 0; i < numSubTree; i++)
			{
				ProcessDirectoryWithSnapshot(@in, v, skipBlocks);
			}
		}

		/// <summary>Process snapshots of a snapshottable directory</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessSnapshots(DataInputStream @in, ImageVisitor v, string rootName
			)
		{
			int numSnapshots = @in.ReadInt();
			if (numSnapshots >= 0)
			{
				v.VisitEnclosingElement(ImageVisitor.ImageElement.Snapshots, ImageVisitor.ImageElement
					.NumSnapshots, numSnapshots);
				for (int i = 0; i < numSnapshots; i++)
				{
					// process snapshot
					v.VisitEnclosingElement(ImageVisitor.ImageElement.Snapshot);
					v.Visit(ImageVisitor.ImageElement.SnapshotId, @in.ReadInt());
					v.LeaveEnclosingElement();
				}
				v.Visit(ImageVisitor.ImageElement.SnapshotQuota, @in.ReadInt());
				v.LeaveEnclosingElement();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessSnapshot(DataInputStream @in, ImageVisitor v)
		{
			v.VisitEnclosingElement(ImageVisitor.ImageElement.Snapshot);
			v.Visit(ImageVisitor.ImageElement.SnapshotId, @in.ReadInt());
			// process root of snapshot
			v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotRoot);
			ProcessINode(@in, v, true, string.Empty, false);
			v.LeaveEnclosingElement();
			v.LeaveEnclosingElement();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessDirectoryDiffList(DataInputStream @in, ImageVisitor v, string
			 currentINodeName)
		{
			int numDirDiff = @in.ReadInt();
			if (numDirDiff >= 0)
			{
				v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotDirDiffs, ImageVisitor.ImageElement
					.NumSnapshotDirDiff, numDirDiff);
				for (int i = 0; i < numDirDiff; i++)
				{
					// process directory diffs in reverse chronological oder
					ProcessDirectoryDiff(@in, v, currentINodeName);
				}
				v.LeaveEnclosingElement();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessDirectoryDiff(DataInputStream @in, ImageVisitor v, string currentINodeName
			)
		{
			v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotDirDiff);
			int snapshotId = @in.ReadInt();
			v.Visit(ImageVisitor.ImageElement.SnapshotDiffSnapshotid, snapshotId);
			v.Visit(ImageVisitor.ImageElement.SnapshotDirDiffChildrenSize, @in.ReadInt());
			// process snapshotINode
			bool useRoot = @in.ReadBoolean();
			if (!useRoot)
			{
				if (@in.ReadBoolean())
				{
					v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotInodeDirectoryAttributes
						);
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.OptimizeSnapshotInodes, 
						imageVersion))
					{
						ProcessINodeDirectoryAttributes(@in, v, currentINodeName);
					}
					else
					{
						ProcessINode(@in, v, true, currentINodeName, true);
					}
					v.LeaveEnclosingElement();
				}
			}
			// process createdList
			int createdSize = @in.ReadInt();
			v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotDirDiffCreatedlist, ImageVisitor.ImageElement
				.SnapshotDirDiffCreatedlistSize, createdSize);
			for (int i = 0; i < createdSize; i++)
			{
				string createdNode = FSImageSerialization.ReadString(@in);
				v.Visit(ImageVisitor.ImageElement.SnapshotDirDiffCreatedInode, createdNode);
			}
			v.LeaveEnclosingElement();
			// process deletedList
			int deletedSize = @in.ReadInt();
			v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotDirDiffDeletedlist, ImageVisitor.ImageElement
				.SnapshotDirDiffDeletedlistSize, deletedSize);
			for (int i_1 = 0; i_1 < deletedSize; i_1++)
			{
				v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotDirDiffDeletedInode);
				ProcessINode(@in, v, false, currentINodeName, true);
				v.LeaveEnclosingElement();
			}
			v.LeaveEnclosingElement();
			v.LeaveEnclosingElement();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessINodeDirectoryAttributes(DataInputStream @in, ImageVisitor v, 
			string parentName)
		{
			string pathName = ReadINodePath(@in, parentName);
			v.Visit(ImageVisitor.ImageElement.InodePath, pathName);
			ProcessPermission(@in, v);
			v.Visit(ImageVisitor.ImageElement.ModificationTime, FormatDate(@in.ReadLong()));
			v.Visit(ImageVisitor.ImageElement.NsQuota, @in.ReadLong());
			v.Visit(ImageVisitor.ImageElement.DsQuota, @in.ReadLong());
		}

		/// <summary>Process children under a directory</summary>
		/// <exception cref="System.IO.IOException"/>
		private int ProcessChildren(DataInputStream @in, ImageVisitor v, bool skipBlocks, 
			string parentName)
		{
			int numChildren = @in.ReadInt();
			for (int i = 0; i < numChildren; i++)
			{
				ProcessINode(@in, v, skipBlocks, parentName, false);
			}
			return numChildren;
		}

		/// <summary>Process image with full path name</summary>
		/// <param name="in">image stream</param>
		/// <param name="v">visitor</param>
		/// <param name="numInodes">number of indoes to read</param>
		/// <param name="skipBlocks">skip blocks or not</param>
		/// <exception cref="System.IO.IOException">if there is any error occurs</exception>
		private void ProcessFullNameINodes(DataInputStream @in, ImageVisitor v, long numInodes
			, bool skipBlocks)
		{
			for (long i = 0; i < numInodes; i++)
			{
				ProcessINode(@in, v, skipBlocks, null, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string ReadINodePath(DataInputStream @in, string parentName)
		{
			string pathName = FSImageSerialization.ReadString(@in);
			if (parentName != null)
			{
				// local name
				pathName = "/" + pathName;
				if (!"/".Equals(parentName))
				{
					// children of non-root directory
					pathName = parentName + pathName;
				}
			}
			return pathName;
		}

		/// <summary>Process an INode</summary>
		/// <param name="in">image stream</param>
		/// <param name="v">visitor</param>
		/// <param name="skipBlocks">skip blocks or not</param>
		/// <param name="parentName">the name of its parent node</param>
		/// <param name="isSnapshotCopy">whether or not the inode is a snapshot copy</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessINode(DataInputStream @in, ImageVisitor v, bool skipBlocks, string
			 parentName, bool isSnapshotCopy)
		{
			bool supportSnapshot = NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot
				, imageVersion);
			bool supportInodeId = NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId
				, imageVersion);
			v.VisitEnclosingElement(ImageVisitor.ImageElement.Inode);
			string pathName = ReadINodePath(@in, parentName);
			v.Visit(ImageVisitor.ImageElement.InodePath, pathName);
			long inodeId = INodeId.GrandfatherInodeId;
			if (supportInodeId)
			{
				inodeId = @in.ReadLong();
				v.Visit(ImageVisitor.ImageElement.InodeId, inodeId);
			}
			v.Visit(ImageVisitor.ImageElement.Replication, @in.ReadShort());
			v.Visit(ImageVisitor.ImageElement.ModificationTime, FormatDate(@in.ReadLong()));
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FileAccessTime, imageVersion
				))
			{
				v.Visit(ImageVisitor.ImageElement.AccessTime, FormatDate(@in.ReadLong()));
			}
			v.Visit(ImageVisitor.ImageElement.BlockSize, @in.ReadLong());
			int numBlocks = @in.ReadInt();
			ProcessBlocks(@in, v, numBlocks, skipBlocks);
			if (numBlocks >= 0)
			{
				// File
				if (supportSnapshot)
				{
					// make sure subtreeMap only contains entry for directory
					Sharpen.Collections.Remove(subtreeMap, inodeId);
					// process file diffs
					ProcessFileDiffList(@in, v, parentName);
					if (isSnapshotCopy)
					{
						bool underConstruction = @in.ReadBoolean();
						if (underConstruction)
						{
							v.Visit(ImageVisitor.ImageElement.ClientName, FSImageSerialization.ReadString(@in
								));
							v.Visit(ImageVisitor.ImageElement.ClientMachine, FSImageSerialization.ReadString(
								@in));
						}
					}
				}
				ProcessPermission(@in, v);
			}
			else
			{
				if (numBlocks == -1)
				{
					// Directory
					if (supportSnapshot && supportInodeId)
					{
						dirNodeMap[inodeId] = pathName;
					}
					v.Visit(ImageVisitor.ImageElement.NsQuota, numBlocks == -1 ? @in.ReadLong() : -1);
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.DiskspaceQuota, imageVersion
						))
					{
						v.Visit(ImageVisitor.ImageElement.DsQuota, numBlocks == -1 ? @in.ReadLong() : -1);
					}
					if (supportSnapshot)
					{
						bool snapshottable = @in.ReadBoolean();
						if (!snapshottable)
						{
							bool withSnapshot = @in.ReadBoolean();
							v.Visit(ImageVisitor.ImageElement.IsWithsnapshotDir, bool.ToString(withSnapshot));
						}
						else
						{
							v.Visit(ImageVisitor.ImageElement.IsSnapshottableDir, bool.ToString(snapshottable
								));
						}
					}
					ProcessPermission(@in, v);
				}
				else
				{
					if (numBlocks == -2)
					{
						v.Visit(ImageVisitor.ImageElement.Symlink, Text.ReadString(@in));
						ProcessPermission(@in, v);
					}
					else
					{
						if (numBlocks == -3)
						{
							// reference node
							bool isWithName = @in.ReadBoolean();
							int snapshotId = @in.ReadInt();
							if (isWithName)
							{
								v.Visit(ImageVisitor.ImageElement.SnapshotLastSnapshotId, snapshotId);
							}
							else
							{
								v.Visit(ImageVisitor.ImageElement.SnapshotDstSnapshotId, snapshotId);
							}
							bool firstReferred = @in.ReadBoolean();
							if (firstReferred)
							{
								// if a subtree is linked by multiple "parents", the corresponding dir
								// must be referred by a reference node. we put the reference node into
								// the subtreeMap here and let its value be false. when we later visit
								// the subtree for the first time, we change the value to true.
								subtreeMap[inodeId] = false;
								v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotRefInode);
								ProcessINode(@in, v, skipBlocks, parentName, isSnapshotCopy);
								v.LeaveEnclosingElement();
							}
							else
							{
								// referred inode    
								v.Visit(ImageVisitor.ImageElement.SnapshotRefInodeId, @in.ReadLong());
							}
						}
					}
				}
			}
			v.LeaveEnclosingElement();
		}

		// INode
		/// <exception cref="System.IO.IOException"/>
		private void ProcessINodeFileAttributes(DataInputStream @in, ImageVisitor v, string
			 parentName)
		{
			string pathName = ReadINodePath(@in, parentName);
			v.Visit(ImageVisitor.ImageElement.InodePath, pathName);
			ProcessPermission(@in, v);
			v.Visit(ImageVisitor.ImageElement.ModificationTime, FormatDate(@in.ReadLong()));
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FileAccessTime, imageVersion
				))
			{
				v.Visit(ImageVisitor.ImageElement.AccessTime, FormatDate(@in.ReadLong()));
			}
			v.Visit(ImageVisitor.ImageElement.Replication, @in.ReadShort());
			v.Visit(ImageVisitor.ImageElement.BlockSize, @in.ReadLong());
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessFileDiffList(DataInputStream @in, ImageVisitor v, string currentINodeName
			)
		{
			int size = @in.ReadInt();
			if (size >= 0)
			{
				v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotFileDiffs, ImageVisitor.ImageElement
					.NumSnapshotFileDiff, size);
				for (int i = 0; i < size; i++)
				{
					ProcessFileDiff(@in, v, currentINodeName);
				}
				v.LeaveEnclosingElement();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessFileDiff(DataInputStream @in, ImageVisitor v, string currentINodeName
			)
		{
			int snapshotId = @in.ReadInt();
			v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotFileDiff, ImageVisitor.ImageElement
				.SnapshotDiffSnapshotid, snapshotId);
			v.Visit(ImageVisitor.ImageElement.SnapshotFileSize, @in.ReadLong());
			if (@in.ReadBoolean())
			{
				v.VisitEnclosingElement(ImageVisitor.ImageElement.SnapshotInodeFileAttributes);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.OptimizeSnapshotInodes, 
					imageVersion))
				{
					ProcessINodeFileAttributes(@in, v, currentINodeName);
				}
				else
				{
					ProcessINode(@in, v, true, currentINodeName, true);
				}
				v.LeaveEnclosingElement();
			}
			v.LeaveEnclosingElement();
		}

		/// <summary>Helper method to format dates during processing.</summary>
		/// <param name="date">Date as read from image file</param>
		/// <returns>String version of date format</returns>
		private string FormatDate(long date)
		{
			return dateFormat.Format(Sharpen.Extensions.CreateDate(date));
		}
	}
}

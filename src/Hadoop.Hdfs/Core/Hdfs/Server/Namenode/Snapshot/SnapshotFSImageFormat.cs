using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>
	/// A helper class defining static methods for reading/writing snapshot related
	/// information from/to FSImage.
	/// </summary>
	public class SnapshotFSImageFormat
	{
		/// <summary>Save snapshots and snapshot quota for a snapshottable directory.</summary>
		/// <param name="current">The directory that the snapshots belongs to.</param>
		/// <param name="out">
		/// The
		/// <see cref="System.IO.DataOutput"/>
		/// to write.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void SaveSnapshots(INodeDirectory current, DataOutput @out)
		{
			DirectorySnapshottableFeature sf = current.GetDirectorySnapshottableFeature();
			Preconditions.CheckArgument(sf != null);
			// list of snapshots in snapshotsByNames
			ReadOnlyList<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot> snapshots = 
				sf.GetSnapshotList();
			@out.WriteInt(snapshots.Size());
			foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s in snapshots)
			{
				// write the snapshot id
				@out.WriteInt(s.GetId());
			}
			// snapshot quota
			@out.WriteInt(sf.GetSnapshotQuota());
		}

		/// <summary>Save SnapshotDiff list for an INodeDirectoryWithSnapshot.</summary>
		/// <param name="sNode">The directory that the SnapshotDiff list belongs to.</param>
		/// <param name="out">
		/// The
		/// <see cref="System.IO.DataOutput"/>
		/// to write.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private static void SaveINodeDiffs<N, A, D>(AbstractINodeDiffList<N, A, D> diffs, 
			DataOutput @out, SnapshotFSImageFormat.ReferenceMap referenceMap)
			where N : INode
			where A : INodeAttributes
			where D : AbstractINodeDiff<N, A, D>
		{
			// Record the diffs in reversed order, so that we can find the correct
			// reference for INodes in the created list when loading the FSImage
			if (diffs == null)
			{
				@out.WriteInt(-1);
			}
			else
			{
				// no diffs
				IList<D> list = diffs.AsList();
				int size = list.Count;
				@out.WriteInt(size);
				for (int i = size - 1; i >= 0; i--)
				{
					list[i].Write(@out, referenceMap);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void SaveDirectoryDiffList(INodeDirectory dir, DataOutput @out, SnapshotFSImageFormat.ReferenceMap
			 referenceMap)
		{
			SaveINodeDiffs(dir.GetDiffs(), @out, referenceMap);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void SaveFileDiffList(INodeFile file, DataOutput @out)
		{
			SaveINodeDiffs(file.GetDiffs(), @out, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static FileDiffList LoadFileDiffList(DataInput @in, FSImageFormat.Loader loader
			)
		{
			int size = @in.ReadInt();
			if (size == -1)
			{
				return null;
			}
			else
			{
				FileDiffList diffs = new FileDiffList();
				FileDiff posterior = null;
				for (int i = 0; i < size; i++)
				{
					FileDiff d = LoadFileDiff(posterior, @in, loader);
					diffs.AddFirst(d);
					posterior = d;
				}
				return diffs;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static FileDiff LoadFileDiff(FileDiff posterior, DataInput @in, FSImageFormat.Loader
			 loader)
		{
			// 1. Read the id of the Snapshot root to identify the Snapshot
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = loader.GetSnapshot
				(@in);
			// 2. Load file size
			long fileSize = @in.ReadLong();
			// 3. Load snapshotINode 
			INodeFileAttributes snapshotINode = @in.ReadBoolean() ? loader.LoadINodeFileAttributes
				(@in) : null;
			return new FileDiff(snapshot.GetId(), snapshotINode, posterior, fileSize);
		}

		/// <summary>Load a node stored in the created list from fsimage.</summary>
		/// <param name="createdNodeName">The name of the created node.</param>
		/// <param name="parent">The directory that the created list belongs to.</param>
		/// <returns>The created node.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static INode LoadCreated(byte[] createdNodeName, INodeDirectory parent)
		{
			// the INode in the created list should be a reference to another INode
			// in posterior SnapshotDiffs or one of the current children
			foreach (DirectoryWithSnapshotFeature.DirectoryDiff postDiff in parent.GetDiffs())
			{
				INode d = postDiff.GetChildrenDiff().Search(Diff.ListType.Deleted, createdNodeName
					);
				if (d != null)
				{
					return d;
				}
			}
			// else go to the next SnapshotDiff
			// use the current child
			INode currentChild = parent.GetChild(createdNodeName, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			if (currentChild == null)
			{
				throw new IOException("Cannot find an INode associated with the INode " + DFSUtil
					.Bytes2String(createdNodeName) + " in created list while loading FSImage.");
			}
			return currentChild;
		}

		/// <summary>Load the created list from fsimage.</summary>
		/// <param name="parent">The directory that the created list belongs to.</param>
		/// <param name="in">
		/// The
		/// <see cref="System.IO.DataInput"/>
		/// to read.
		/// </param>
		/// <returns>The created list.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static IList<INode> LoadCreatedList(INodeDirectory parent, DataInput @in)
		{
			// read the size of the created list
			int createdSize = @in.ReadInt();
			IList<INode> createdList = new AList<INode>(createdSize);
			for (int i = 0; i < createdSize; i++)
			{
				byte[] createdNodeName = FSImageSerialization.ReadLocalName(@in);
				INode created = LoadCreated(createdNodeName, parent);
				createdList.AddItem(created);
			}
			return createdList;
		}

		/// <summary>Load the deleted list from the fsimage.</summary>
		/// <param name="parent">The directory that the deleted list belongs to.</param>
		/// <param name="createdList">
		/// The created list associated with the deleted list in
		/// the same Diff.
		/// </param>
		/// <param name="in">
		/// The
		/// <see cref="System.IO.DataInput"/>
		/// to read.
		/// </param>
		/// <param name="loader">
		/// The
		/// <see cref="Loader"/>
		/// instance.
		/// </param>
		/// <returns>The deleted list.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static IList<INode> LoadDeletedList(INodeDirectory parent, IList<INode> createdList
			, DataInput @in, FSImageFormat.Loader loader)
		{
			int deletedSize = @in.ReadInt();
			IList<INode> deletedList = new AList<INode>(deletedSize);
			for (int i = 0; i < deletedSize; i++)
			{
				INode deleted = loader.LoadINodeWithLocalName(true, @in, true);
				deletedList.AddItem(deleted);
				// set parent: the parent field of an INode in the deleted list is not 
				// useful, but set the parent here to be consistent with the original 
				// fsdir tree.
				deleted.SetParent(parent);
				if (deleted.IsFile())
				{
					loader.UpdateBlocksMap(deleted.AsFile());
				}
			}
			return deletedList;
		}

		/// <summary>Load snapshots and snapshotQuota for a Snapshottable directory.</summary>
		/// <param name="snapshottableParent">The snapshottable directory for loading.</param>
		/// <param name="numSnapshots">The number of snapshots that the directory has.</param>
		/// <param name="loader">The loader</param>
		/// <exception cref="System.IO.IOException"/>
		public static void LoadSnapshotList(INodeDirectory snapshottableParent, int numSnapshots
			, DataInput @in, FSImageFormat.Loader loader)
		{
			DirectorySnapshottableFeature sf = snapshottableParent.GetDirectorySnapshottableFeature
				();
			Preconditions.CheckArgument(sf != null);
			for (int i = 0; i < numSnapshots; i++)
			{
				// read snapshots
				Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s = loader.GetSnapshot(@in
					);
				s.GetRoot().SetParent(snapshottableParent);
				sf.AddSnapshot(s);
			}
			int snapshotQuota = @in.ReadInt();
			snapshottableParent.SetSnapshotQuota(snapshotQuota);
		}

		/// <summary>
		/// Load the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Tools.Snapshot.SnapshotDiff"/>
		/// list for the INodeDirectoryWithSnapshot
		/// directory.
		/// </summary>
		/// <param name="dir">The snapshottable directory for loading.</param>
		/// <param name="in">
		/// The
		/// <see cref="System.IO.DataInput"/>
		/// instance to read.
		/// </param>
		/// <param name="loader">The loader</param>
		/// <exception cref="System.IO.IOException"/>
		public static void LoadDirectoryDiffList(INodeDirectory dir, DataInput @in, FSImageFormat.Loader
			 loader)
		{
			int size = @in.ReadInt();
			if (dir.IsWithSnapshot())
			{
				DirectoryWithSnapshotFeature.DirectoryDiffList diffs = dir.GetDiffs();
				for (int i = 0; i < size; i++)
				{
					diffs.AddFirst(LoadDirectoryDiff(dir, @in, loader));
				}
			}
		}

		/// <summary>
		/// Load the snapshotINode field of
		/// <see cref="AbstractINodeDiff{N, A, D}"/>
		/// .
		/// </summary>
		/// <param name="snapshot">
		/// The Snapshot associated with the
		/// <see cref="AbstractINodeDiff{N, A, D}"/>
		/// .
		/// </param>
		/// <param name="in">
		/// The
		/// <see cref="System.IO.DataInput"/>
		/// to read.
		/// </param>
		/// <param name="loader">
		/// The
		/// <see cref="Loader"/>
		/// instance that this loading procedure is
		/// using.
		/// </param>
		/// <returns>The snapshotINode.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static INodeDirectoryAttributes LoadSnapshotINodeInDirectoryDiff(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 snapshot, DataInput @in, FSImageFormat.Loader loader)
		{
			// read the boolean indicating whether snapshotINode == Snapshot.Root
			bool useRoot = @in.ReadBoolean();
			if (useRoot)
			{
				return snapshot.GetRoot();
			}
			else
			{
				// another boolean is used to indicate whether snapshotINode is non-null
				return @in.ReadBoolean() ? loader.LoadINodeDirectoryAttributes(@in) : null;
			}
		}

		/// <summary>
		/// Load
		/// <see cref="DirectoryDiff"/>
		/// from fsimage.
		/// </summary>
		/// <param name="parent">The directory that the SnapshotDiff belongs to.</param>
		/// <param name="in">
		/// The
		/// <see cref="System.IO.DataInput"/>
		/// instance to read.
		/// </param>
		/// <param name="loader">
		/// The
		/// <see cref="Loader"/>
		/// instance that this loading procedure is
		/// using.
		/// </param>
		/// <returns>
		/// A
		/// <see cref="DirectoryDiff"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private static DirectoryWithSnapshotFeature.DirectoryDiff LoadDirectoryDiff(INodeDirectory
			 parent, DataInput @in, FSImageFormat.Loader loader)
		{
			// 1. Read the full path of the Snapshot root to identify the Snapshot
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = loader.GetSnapshot
				(@in);
			// 2. Load DirectoryDiff#childrenSize
			int childrenSize = @in.ReadInt();
			// 3. Load DirectoryDiff#snapshotINode 
			INodeDirectoryAttributes snapshotINode = LoadSnapshotINodeInDirectoryDiff(snapshot
				, @in, loader);
			// 4. Load the created list in SnapshotDiff#Diff
			IList<INode> createdList = LoadCreatedList(parent, @in);
			// 5. Load the deleted list in SnapshotDiff#Diff
			IList<INode> deletedList = LoadDeletedList(parent, createdList, @in, loader);
			// 6. Compose the SnapshotDiff
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffs = parent.GetDiffs().AsList
				();
			DirectoryWithSnapshotFeature.DirectoryDiff sdiff = new DirectoryWithSnapshotFeature.DirectoryDiff
				(snapshot.GetId(), snapshotINode, diffs.IsEmpty() ? null : diffs[0], childrenSize
				, createdList, deletedList, snapshotINode == snapshot.GetRoot());
			return sdiff;
		}

		/// <summary>A reference map for fsimage serialization.</summary>
		public class ReferenceMap
		{
			/// <summary>Used to indicate whether the reference node itself has been saved</summary>
			private readonly IDictionary<long, INodeReference.WithCount> referenceMap = new Dictionary
				<long, INodeReference.WithCount>();

			/// <summary>Used to record whether the subtree of the reference node has been saved</summary>
			private readonly IDictionary<long, long> dirMap = new Dictionary<long, long>();

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteINodeReferenceWithCount(INodeReference.WithCount withCount
				, DataOutput @out, bool writeUnderConstruction)
			{
				INode referred = withCount.GetReferredINode();
				long id = withCount.GetId();
				bool firstReferred = !referenceMap.Contains(id);
				@out.WriteBoolean(firstReferred);
				if (firstReferred)
				{
					FSImageSerialization.SaveINode2Image(referred, @out, writeUnderConstruction, this
						);
					referenceMap[id] = withCount;
				}
				else
				{
					@out.WriteLong(id);
				}
			}

			public virtual bool ToProcessSubtree(long id)
			{
				if (dirMap.Contains(id))
				{
					return false;
				}
				else
				{
					dirMap[id] = id;
					return true;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual INodeReference.WithCount LoadINodeReferenceWithCount(bool isSnapshotINode
				, DataInput @in, FSImageFormat.Loader loader)
			{
				bool firstReferred = @in.ReadBoolean();
				INodeReference.WithCount withCount;
				if (firstReferred)
				{
					INode referred = loader.LoadINodeWithLocalName(isSnapshotINode, @in, true);
					withCount = new INodeReference.WithCount(null, referred);
					referenceMap[withCount.GetId()] = withCount;
				}
				else
				{
					long id = @in.ReadLong();
					withCount = referenceMap[id];
				}
				return withCount;
			}
		}
	}
}

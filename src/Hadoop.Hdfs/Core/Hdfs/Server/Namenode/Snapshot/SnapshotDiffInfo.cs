using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>
	/// A class describing the difference between snapshots of a snapshottable
	/// directory.
	/// </summary>
	internal class SnapshotDiffInfo
	{
		private sealed class _IComparer_48 : IComparer<INode>
		{
			public _IComparer_48()
			{
			}

			public int Compare(INode left, INode right)
			{
				if (left == null)
				{
					return right == null ? 0 : -1;
				}
				else
				{
					if (right == null)
					{
						return 1;
					}
					else
					{
						int cmp = this.Compare(left.GetParent(), right.GetParent());
						return cmp == 0 ? SignedBytes.LexicographicalComparator().Compare(left.GetLocalNameBytes
							(), right.GetLocalNameBytes()) : cmp;
					}
				}
			}
		}

		/// <summary>Compare two inodes based on their full names</summary>
		public static readonly IComparer<INode> InodeComparator = new _IComparer_48();

		internal class RenameEntry
		{
			private byte[][] sourcePath;

			private byte[][] targetPath;

			internal virtual void SetSource(INode source, byte[][] sourceParentPath)
			{
				Preconditions.CheckState(sourcePath == null);
				sourcePath = new byte[sourceParentPath.Length + 1][];
				System.Array.Copy(sourceParentPath, 0, sourcePath, 0, sourceParentPath.Length);
				sourcePath[sourcePath.Length - 1] = source.GetLocalNameBytes();
			}

			internal virtual void SetTarget(INode target, byte[][] targetParentPath)
			{
				targetPath = new byte[targetParentPath.Length + 1][];
				System.Array.Copy(targetParentPath, 0, targetPath, 0, targetParentPath.Length);
				targetPath[targetPath.Length - 1] = target.GetLocalNameBytes();
			}

			internal virtual void SetTarget(byte[][] targetPath)
			{
				this.targetPath = targetPath;
			}

			internal virtual bool IsRename()
			{
				return sourcePath != null && targetPath != null;
			}

			internal virtual byte[][] GetSourcePath()
			{
				return sourcePath;
			}

			internal virtual byte[][] GetTargetPath()
			{
				return targetPath;
			}
		}

		/// <summary>The root directory of the snapshots</summary>
		private readonly INodeDirectory snapshotRoot;

		/// <summary>The starting point of the difference</summary>
		private readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot from;

		/// <summary>The end point of the difference</summary>
		private readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot to;

		/// <summary>
		/// A map recording modified INodeFile and INodeDirectory and their relative
		/// path corresponding to the snapshot root.
		/// </summary>
		/// <remarks>
		/// A map recording modified INodeFile and INodeDirectory and their relative
		/// path corresponding to the snapshot root. Sorted based on their names.
		/// </remarks>
		private readonly SortedDictionary<INode, byte[][]> diffMap = new SortedDictionary
			<INode, byte[][]>(InodeComparator);

		/// <summary>A map capturing the detailed difference about file creation/deletion.</summary>
		/// <remarks>
		/// A map capturing the detailed difference about file creation/deletion.
		/// Each key indicates a directory whose children have been changed between
		/// the two snapshots, while its associated value is a
		/// <see cref="ChildrenDiff"/>
		/// storing the changes (creation/deletion) happened to the children (files).
		/// </remarks>
		private readonly IDictionary<INodeDirectory, DirectoryWithSnapshotFeature.ChildrenDiff
			> dirDiffMap = new Dictionary<INodeDirectory, DirectoryWithSnapshotFeature.ChildrenDiff
			>();

		private readonly IDictionary<long, SnapshotDiffInfo.RenameEntry> renameMap = new 
			Dictionary<long, SnapshotDiffInfo.RenameEntry>();

		internal SnapshotDiffInfo(INodeDirectory snapshotRoot, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 start, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot end)
		{
			Preconditions.CheckArgument(snapshotRoot.IsSnapshottable());
			this.snapshotRoot = snapshotRoot;
			this.from = start;
			this.to = end;
		}

		/// <summary>Add a dir-diff pair</summary>
		internal virtual void AddDirDiff(INodeDirectory dir, byte[][] relativePath, DirectoryWithSnapshotFeature.ChildrenDiff
			 diff)
		{
			dirDiffMap[dir] = diff;
			diffMap[dir] = relativePath;
			// detect rename
			foreach (INode created in diff.GetList(Diff.ListType.Created))
			{
				if (created.IsReference())
				{
					SnapshotDiffInfo.RenameEntry entry = GetEntry(created.GetId());
					if (entry.GetTargetPath() == null)
					{
						entry.SetTarget(created, relativePath);
					}
				}
			}
			foreach (INode deleted in diff.GetList(Diff.ListType.Deleted))
			{
				if (deleted is INodeReference.WithName)
				{
					SnapshotDiffInfo.RenameEntry entry = GetEntry(deleted.GetId());
					entry.SetSource(deleted, relativePath);
				}
			}
		}

		internal virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetFrom
			()
		{
			return from;
		}

		internal virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetTo()
		{
			return to;
		}

		private SnapshotDiffInfo.RenameEntry GetEntry(long inodeId)
		{
			SnapshotDiffInfo.RenameEntry entry = renameMap[inodeId];
			if (entry == null)
			{
				entry = new SnapshotDiffInfo.RenameEntry();
				renameMap[inodeId] = entry;
			}
			return entry;
		}

		internal virtual void SetRenameTarget(long inodeId, byte[][] path)
		{
			GetEntry(inodeId).SetTarget(path);
		}

		/// <summary>Add a modified file</summary>
		internal virtual void AddFileDiff(INodeFile file, byte[][] relativePath)
		{
			diffMap[file] = relativePath;
		}

		/// <returns>
		/// True if
		/// <see cref="from"/>
		/// is earlier than
		/// <see cref="to"/>
		/// 
		/// </returns>
		internal virtual bool IsFromEarlier()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdComparator.Compare
				(from, to) < 0;
		}

		/// <summary>
		/// Generate a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotDiffReport"/>
		/// based on detailed diff information.
		/// </summary>
		/// <returns>
		/// A
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotDiffReport"/>
		/// describing the difference
		/// </returns>
		public virtual SnapshotDiffReport GenerateReport()
		{
			IList<SnapshotDiffReport.DiffReportEntry> diffReportList = new AList<SnapshotDiffReport.DiffReportEntry
				>();
			foreach (KeyValuePair<INode, byte[][]> drEntry in diffMap)
			{
				INode node = drEntry.Key;
				byte[][] path = drEntry.Value;
				diffReportList.AddItem(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
					.Modify, path, null));
				if (node.IsDirectory())
				{
					IList<SnapshotDiffReport.DiffReportEntry> subList = GenerateReport(dirDiffMap[node
						], path, IsFromEarlier(), renameMap);
					Sharpen.Collections.AddAll(diffReportList, subList);
				}
			}
			return new SnapshotDiffReport(snapshotRoot.GetFullPathName(), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotName(from), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotName(to), diffReportList);
		}

		/// <summary>
		/// Interpret the ChildrenDiff and generate a list of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotDiffReport.DiffReportEntry"/>
		/// .
		/// </summary>
		/// <param name="dirDiff">The ChildrenDiff.</param>
		/// <param name="parentPath">The relative path of the parent.</param>
		/// <param name="fromEarlier">
		/// True indicates
		/// <c>diff=later-earlier</c>
		/// ,
		/// False indicates
		/// <c>diff=earlier-later</c>
		/// </param>
		/// <param name="renameMap">A map containing information about rename operations.</param>
		/// <returns>
		/// A list of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotDiffReport.DiffReportEntry"/>
		/// as the diff report.
		/// </returns>
		private IList<SnapshotDiffReport.DiffReportEntry> GenerateReport(DirectoryWithSnapshotFeature.ChildrenDiff
			 dirDiff, byte[][] parentPath, bool fromEarlier, IDictionary<long, SnapshotDiffInfo.RenameEntry
			> renameMap)
		{
			IList<SnapshotDiffReport.DiffReportEntry> list = new AList<SnapshotDiffReport.DiffReportEntry
				>();
			IList<INode> created = dirDiff.GetList(Diff.ListType.Created);
			IList<INode> deleted = dirDiff.GetList(Diff.ListType.Deleted);
			byte[][] fullPath = new byte[parentPath.Length + 1][];
			System.Array.Copy(parentPath, 0, fullPath, 0, parentPath.Length);
			foreach (INode cnode in created)
			{
				SnapshotDiffInfo.RenameEntry entry = renameMap[cnode.GetId()];
				if (entry == null || !entry.IsRename())
				{
					fullPath[fullPath.Length - 1] = cnode.GetLocalNameBytes();
					list.AddItem(new SnapshotDiffReport.DiffReportEntry(fromEarlier ? SnapshotDiffReport.DiffType
						.Create : SnapshotDiffReport.DiffType.Delete, fullPath));
				}
			}
			foreach (INode dnode in deleted)
			{
				SnapshotDiffInfo.RenameEntry entry = renameMap[dnode.GetId()];
				if (entry != null && entry.IsRename())
				{
					list.AddItem(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Rename
						, fromEarlier ? entry.GetSourcePath() : entry.GetTargetPath(), fromEarlier ? entry
						.GetTargetPath() : entry.GetSourcePath()));
				}
				else
				{
					fullPath[fullPath.Length - 1] = dnode.GetLocalNameBytes();
					list.AddItem(new SnapshotDiffReport.DiffReportEntry(fromEarlier ? SnapshotDiffReport.DiffType
						.Delete : SnapshotDiffReport.DiffType.Create, fullPath));
				}
			}
			return list;
		}
	}
}

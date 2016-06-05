using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Snapshot of a sub-tree in the namesystem.</summary>
	public class Snapshot : Comparable<byte[]>
	{
		/// <summary>This id is used to indicate the current state (vs.</summary>
		/// <remarks>This id is used to indicate the current state (vs. snapshots)</remarks>
		public const int CurrentStateId = int.MaxValue - 1;

		public const int NoSnapshotId = -1;

		/// <summary>The pattern for generating the default snapshot name.</summary>
		/// <remarks>
		/// The pattern for generating the default snapshot name.
		/// E.g. s20130412-151029.033
		/// </remarks>
		private const string DefaultSnapshotNamePattern = "'s'yyyyMMdd-HHmmss.SSS";

		public static string GenerateDefaultSnapshotName()
		{
			return new SimpleDateFormat(DefaultSnapshotNamePattern).Format(new DateTime());
		}

		public static string GetSnapshotPath(string snapshottableDir, string snapshotRelativePath
			)
		{
			StringBuilder b = new StringBuilder(snapshottableDir);
			if (b[b.Length - 1] != Path.SeparatorChar)
			{
				b.Append(Path.Separator);
			}
			return b.Append(HdfsConstants.DotSnapshotDir).Append(Path.Separator).Append(snapshotRelativePath
				).ToString();
		}

		/// <summary>Get the name of the given snapshot.</summary>
		/// <param name="s">The given snapshot.</param>
		/// <returns>
		/// The name of the snapshot, or an empty string if
		/// <paramref name="s"/>
		/// is null
		/// </returns>
		internal static string GetSnapshotName(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 s)
		{
			return s != null ? s.GetRoot().GetLocalName() : string.Empty;
		}

		public static int GetSnapshotId(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 s)
		{
			return s == null ? CurrentStateId : s.GetId();
		}

		private sealed class _IComparer_94 : IComparer<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			>
		{
			public _IComparer_94()
			{
			}

			public int Compare(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot left, 
				Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot right)
			{
				return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdIntegerComparator
					.Compare(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotId(
					left), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotId(right
					));
			}
		}

		/// <summary>
		/// Compare snapshot with IDs, where null indicates the current status thus
		/// is greater than any non-null snapshot.
		/// </summary>
		public static readonly IComparer<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			> IdComparator = new _IComparer_94();

		private sealed class _IComparer_107 : IComparer<int>
		{
			public _IComparer_107()
			{
			}

			public int Compare(int left, int right)
			{
				// Snapshot.CURRENT_STATE_ID means the current state, thus should be the 
				// largest
				return left - right;
			}
		}

		/// <summary>
		/// Compare snapshot with IDs, where null indicates the current status thus
		/// is greater than any non-null ID.
		/// </summary>
		public static readonly IComparer<int> IdIntegerComparator = new _IComparer_107();

		/// <summary>
		/// Find the latest snapshot that 1) covers the given inode (which means the
		/// snapshot was either taken on the inode or taken on an ancestor of the
		/// inode), and 2) was taken before the given snapshot (if the given snapshot
		/// is not null).
		/// </summary>
		/// <param name="inode">the given inode that the returned snapshot needs to cover</param>
		/// <param name="anchor">the returned snapshot should be taken before this given id.</param>
		/// <returns>
		/// id of the latest snapshot that covers the given inode and was taken
		/// before the the given snapshot (if it is not null).
		/// </returns>
		public static int FindLatestSnapshot(INode inode, int anchor)
		{
			int latest = NoSnapshotId;
			for (; inode != null; inode = inode.GetParent())
			{
				if (inode.IsDirectory())
				{
					INodeDirectory dir = inode.AsDirectory();
					if (dir.IsWithSnapshot())
					{
						latest = dir.GetDiffs().UpdatePrior(anchor, latest);
					}
				}
			}
			return latest;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot Read(DataInput
			 @in, FSImageFormat.Loader loader)
		{
			int snapshotId = @in.ReadInt();
			INode root = loader.LoadINodeWithLocalName(false, @in, false);
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot(snapshotId, root
				.AsDirectory(), null);
		}

		/// <summary>The root directory of the snapshot.</summary>
		public class Root : INodeDirectory
		{
			internal Root(INodeDirectory other)
				: base(other, false, Sharpen.Collections.ToArray(Lists.NewArrayList(Iterables.Filter
					(Arrays.AsList(other.GetFeatures()), new _Predicate_152())), new INode.Feature[0
					]))
			{
			}

			private sealed class _Predicate_152 : Predicate<INode.Feature>
			{
				public _Predicate_152()
				{
				}

				// Always preserve ACL, XAttr.
				public bool Apply(INode.Feature input)
				{
					if (typeof(AclFeature).IsInstanceOfType(input) || typeof(XAttrFeature).IsInstanceOfType
						(input))
					{
						return true;
					}
					return false;
				}
			}

			public override ReadOnlyList<INode> GetChildrenList(int snapshotId)
			{
				return GetParent().GetChildrenList(snapshotId);
			}

			public override INode GetChild(byte[] name, int snapshotId)
			{
				return GetParent().GetChild(name, snapshotId);
			}

			public override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
				 summary)
			{
				int snapshotId = GetParent().GetSnapshot(GetLocalNameBytes()).GetId();
				return ComputeDirectoryContentSummary(summary, snapshotId);
			}

			public override string GetFullPathName()
			{
				return GetSnapshotPath(GetParent().GetFullPathName(), GetLocalName());
			}
		}

		/// <summary>Snapshot ID.</summary>
		private readonly int id;

		/// <summary>The root directory of the snapshot.</summary>
		private readonly Snapshot.Root root;

		internal Snapshot(int id, string name, INodeDirectory dir)
			: this(id, dir, dir)
		{
			this.root.SetLocalName(DFSUtil.String2Bytes(name));
		}

		internal Snapshot(int id, INodeDirectory dir, INodeDirectory parent)
		{
			this.id = id;
			this.root = new Snapshot.Root(dir);
			this.root.SetParent(parent);
		}

		public virtual int GetId()
		{
			return id;
		}

		/// <returns>the root directory of the snapshot.</returns>
		public virtual Snapshot.Root GetRoot()
		{
			return root;
		}

		public virtual int CompareTo(byte[] bytes)
		{
			return root.CompareTo(bytes);
		}

		public override bool Equals(object that)
		{
			if (this == that)
			{
				return true;
			}
			else
			{
				if (that == null || !(that is Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					))
				{
					return false;
				}
			}
			return this.id == ((Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot)that
				).id;
		}

		public override int GetHashCode()
		{
			return id;
		}

		public override string ToString()
		{
			return GetType().Name + "." + root.GetLocalName() + "(id=" + id + ")";
		}

		/// <summary>Serialize the fields to out</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Write(DataOutput @out)
		{
			@out.WriteInt(id);
			// write root
			FSImageSerialization.WriteINodeDirectory(root, @out);
		}
	}
}

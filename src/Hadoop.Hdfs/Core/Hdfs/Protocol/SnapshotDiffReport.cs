using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// This class represents to end users the difference between two snapshots of
	/// the same directory, or the difference between a snapshot of the directory and
	/// its current state.
	/// </summary>
	/// <remarks>
	/// This class represents to end users the difference between two snapshots of
	/// the same directory, or the difference between a snapshot of the directory and
	/// its current state. Instead of capturing all the details of the diff, this
	/// class only lists where the changes happened and their types.
	/// </remarks>
	public class SnapshotDiffReport
	{
		private static readonly string LineSeparator = Runtime.GetProperty("line.separator"
			, "\n");

		/// <summary>Types of the difference, which include CREATE, MODIFY, DELETE, and RENAME.
		/// 	</summary>
		/// <remarks>
		/// Types of the difference, which include CREATE, MODIFY, DELETE, and RENAME.
		/// Each type has a label for representation: +/M/-/R represent CREATE, MODIFY,
		/// DELETE, and RENAME respectively.
		/// </remarks>
		[System.Serializable]
		public sealed class DiffType
		{
			public static readonly SnapshotDiffReport.DiffType Create = new SnapshotDiffReport.DiffType
				("+");

			public static readonly SnapshotDiffReport.DiffType Modify = new SnapshotDiffReport.DiffType
				("M");

			public static readonly SnapshotDiffReport.DiffType Delete = new SnapshotDiffReport.DiffType
				("-");

			public static readonly SnapshotDiffReport.DiffType Rename = new SnapshotDiffReport.DiffType
				("R");

			private readonly string label;

			private DiffType(string label)
			{
				this.label = label;
			}

			public string GetLabel()
			{
				return SnapshotDiffReport.DiffType.label;
			}

			public static SnapshotDiffReport.DiffType GetTypeFromLabel(string label)
			{
				if (label.Equals(SnapshotDiffReport.DiffType.Create.GetLabel()))
				{
					return SnapshotDiffReport.DiffType.Create;
				}
				else
				{
					if (label.Equals(SnapshotDiffReport.DiffType.Modify.GetLabel()))
					{
						return SnapshotDiffReport.DiffType.Modify;
					}
					else
					{
						if (label.Equals(SnapshotDiffReport.DiffType.Delete.GetLabel()))
						{
							return SnapshotDiffReport.DiffType.Delete;
						}
						else
						{
							if (label.Equals(SnapshotDiffReport.DiffType.Rename.GetLabel()))
							{
								return SnapshotDiffReport.DiffType.Rename;
							}
						}
					}
				}
				return null;
			}
		}

		/// <summary>
		/// Representing the full path and diff type of a file/directory where changes
		/// have happened.
		/// </summary>
		public class DiffReportEntry
		{
			/// <summary>The type of the difference.</summary>
			private readonly SnapshotDiffReport.DiffType type;

			/// <summary>
			/// The relative path (related to the snapshot root) of 1) the file/directory
			/// where changes have happened, or 2) the source file/dir of a rename op.
			/// </summary>
			private readonly byte[] sourcePath;

			private readonly byte[] targetPath;

			public DiffReportEntry(SnapshotDiffReport.DiffType type, byte[] sourcePath)
				: this(type, sourcePath, null)
			{
			}

			public DiffReportEntry(SnapshotDiffReport.DiffType type, byte[][] sourcePathComponents
				)
				: this(type, sourcePathComponents, null)
			{
			}

			public DiffReportEntry(SnapshotDiffReport.DiffType type, byte[] sourcePath, byte[]
				 targetPath)
			{
				this.type = type;
				this.sourcePath = sourcePath;
				this.targetPath = targetPath;
			}

			public DiffReportEntry(SnapshotDiffReport.DiffType type, byte[][] sourcePathComponents
				, byte[][] targetPathComponents)
			{
				this.type = type;
				this.sourcePath = DFSUtil.ByteArray2bytes(sourcePathComponents);
				this.targetPath = targetPathComponents == null ? null : DFSUtil.ByteArray2bytes(targetPathComponents
					);
			}

			public override string ToString()
			{
				string str = type.GetLabel() + "\t" + GetPathString(sourcePath);
				if (type == SnapshotDiffReport.DiffType.Rename)
				{
					str += " -> " + GetPathString(targetPath);
				}
				return str;
			}

			public virtual SnapshotDiffReport.DiffType GetType()
			{
				return type;
			}

			internal static string GetPathString(byte[] path)
			{
				string pathStr = DFSUtil.Bytes2String(path);
				if (pathStr.IsEmpty())
				{
					return Path.CurDir;
				}
				else
				{
					return Path.CurDir + Path.Separator + pathStr;
				}
			}

			public virtual byte[] GetSourcePath()
			{
				return sourcePath;
			}

			public virtual byte[] GetTargetPath()
			{
				return targetPath;
			}

			public override bool Equals(object other)
			{
				if (this == other)
				{
					return true;
				}
				if (other != null && other is SnapshotDiffReport.DiffReportEntry)
				{
					SnapshotDiffReport.DiffReportEntry entry = (SnapshotDiffReport.DiffReportEntry)other;
					return type.Equals(entry.GetType()) && Arrays.Equals(sourcePath, entry.GetSourcePath
						()) && Arrays.Equals(targetPath, entry.GetTargetPath());
				}
				return false;
			}

			public override int GetHashCode()
			{
				return Objects.HashCode(GetSourcePath(), GetTargetPath());
			}
		}

		/// <summary>snapshot root full path</summary>
		private readonly string snapshotRoot;

		/// <summary>start point of the diff</summary>
		private readonly string fromSnapshot;

		/// <summary>end point of the diff</summary>
		private readonly string toSnapshot;

		/// <summary>list of diff</summary>
		private readonly IList<SnapshotDiffReport.DiffReportEntry> diffList;

		public SnapshotDiffReport(string snapshotRoot, string fromSnapshot, string toSnapshot
			, IList<SnapshotDiffReport.DiffReportEntry> entryList)
		{
			this.snapshotRoot = snapshotRoot;
			this.fromSnapshot = fromSnapshot;
			this.toSnapshot = toSnapshot;
			this.diffList = entryList != null ? entryList : Sharpen.Collections.EmptyList<SnapshotDiffReport.DiffReportEntry
				>();
		}

		/// <returns>
		/// 
		/// <see cref="snapshotRoot"/>
		/// 
		/// </returns>
		public virtual string GetSnapshotRoot()
		{
			return snapshotRoot;
		}

		/// <returns>
		/// 
		/// <see cref="fromSnapshot"/>
		/// 
		/// </returns>
		public virtual string GetFromSnapshot()
		{
			return fromSnapshot;
		}

		/// <returns>
		/// 
		/// <see cref="toSnapshot"/>
		/// 
		/// </returns>
		public virtual string GetLaterSnapshotName()
		{
			return toSnapshot;
		}

		/// <returns>
		/// 
		/// <see cref="diffList"/>
		/// 
		/// </returns>
		public virtual IList<SnapshotDiffReport.DiffReportEntry> GetDiffList()
		{
			return diffList;
		}

		public override string ToString()
		{
			StringBuilder str = new StringBuilder();
			string from = fromSnapshot == null || fromSnapshot.IsEmpty() ? "current directory"
				 : "snapshot " + fromSnapshot;
			string to = toSnapshot == null || toSnapshot.IsEmpty() ? "current directory" : "snapshot "
				 + toSnapshot;
			str.Append("Difference between " + from + " and " + to + " under directory " + snapshotRoot
				 + ":" + LineSeparator);
			foreach (SnapshotDiffReport.DiffReportEntry entry in diffList)
			{
				str.Append(entry.ToString() + LineSeparator);
			}
			return str.ToString();
		}
	}
}

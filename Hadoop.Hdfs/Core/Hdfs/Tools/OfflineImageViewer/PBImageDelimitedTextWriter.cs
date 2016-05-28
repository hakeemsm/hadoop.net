using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// A PBImageDelimitedTextWriter generates a text representation of the PB fsimage,
	/// with each element separated by a delimiter string.
	/// </summary>
	/// <remarks>
	/// A PBImageDelimitedTextWriter generates a text representation of the PB fsimage,
	/// with each element separated by a delimiter string.  All of the elements
	/// common to both inodes and inodes-under-construction are included. When
	/// processing an fsimage with a layout version that did not include an
	/// element, such as AccessTime, the output file will include a column
	/// for the value, but no value will be included.
	/// Individual block information for each file is not currently included.
	/// The default delimiter is tab, as this is an unlikely value to be included in
	/// an inode path or other text metadata. The delimiter value can be via the
	/// constructor.
	/// </remarks>
	public class PBImageDelimitedTextWriter : PBImageTextWriter
	{
		internal const string DefaultDelimiter = "\t";

		private const string DateFormat = "yyyy-MM-dd HH:mm";

		private readonly SimpleDateFormat dateFormatter = new SimpleDateFormat(DateFormat
			);

		private readonly string delimiter;

		/// <exception cref="System.IO.IOException"/>
		internal PBImageDelimitedTextWriter(TextWriter @out, string delimiter, string tempPath
			)
			: base(@out, tempPath)
		{
			this.delimiter = delimiter;
		}

		private string FormatDate(long date)
		{
			return dateFormatter.Format(Sharpen.Extensions.CreateDate(date));
		}

		private void Append(StringBuilder buffer, int field)
		{
			buffer.Append(delimiter);
			buffer.Append(field);
		}

		private void Append(StringBuilder buffer, long field)
		{
			buffer.Append(delimiter);
			buffer.Append(field);
		}

		private void Append(StringBuilder buffer, string field)
		{
			buffer.Append(delimiter);
			buffer.Append(field);
		}

		protected internal override string GetEntry(string parent, FsImageProto.INodeSection.INode
			 inode)
		{
			StringBuilder buffer = new StringBuilder();
			string inodeName = inode.GetName().ToStringUtf8();
			Path path = new Path(parent.IsEmpty() ? "/" : parent, inodeName.IsEmpty() ? "/" : 
				inodeName);
			buffer.Append(path.ToString());
			PermissionStatus p = null;
			switch (inode.GetType())
			{
				case FsImageProto.INodeSection.INode.Type.File:
				{
					FsImageProto.INodeSection.INodeFile file = inode.GetFile();
					p = GetPermission(file.GetPermission());
					Append(buffer, file.GetReplication());
					Append(buffer, FormatDate(file.GetModificationTime()));
					Append(buffer, FormatDate(file.GetAccessTime()));
					Append(buffer, file.GetPreferredBlockSize());
					Append(buffer, file.GetBlocksCount());
					Append(buffer, FSImageLoader.GetFileSize(file));
					Append(buffer, 0);
					// NS_QUOTA
					Append(buffer, 0);
					// DS_QUOTA
					break;
				}

				case FsImageProto.INodeSection.INode.Type.Directory:
				{
					FsImageProto.INodeSection.INodeDirectory dir = inode.GetDirectory();
					p = GetPermission(dir.GetPermission());
					Append(buffer, 0);
					// Replication
					Append(buffer, FormatDate(dir.GetModificationTime()));
					Append(buffer, FormatDate(0));
					// Access time.
					Append(buffer, 0);
					// Block size.
					Append(buffer, 0);
					// Num blocks.
					Append(buffer, 0);
					// Num bytes.
					Append(buffer, dir.GetNsQuota());
					Append(buffer, dir.GetDsQuota());
					break;
				}

				case FsImageProto.INodeSection.INode.Type.Symlink:
				{
					FsImageProto.INodeSection.INodeSymlink s = inode.GetSymlink();
					p = GetPermission(s.GetPermission());
					Append(buffer, 0);
					// Replication
					Append(buffer, FormatDate(s.GetModificationTime()));
					Append(buffer, FormatDate(s.GetAccessTime()));
					Append(buffer, 0);
					// Block size.
					Append(buffer, 0);
					// Num blocks.
					Append(buffer, 0);
					// Num bytes.
					Append(buffer, 0);
					// NS_QUOTA
					Append(buffer, 0);
					// DS_QUOTA
					break;
				}

				default:
				{
					break;
				}
			}
			System.Diagnostics.Debug.Assert(p != null);
			Append(buffer, p.GetPermission().ToString());
			Append(buffer, p.GetUserName());
			Append(buffer, p.GetGroupName());
			return buffer.ToString();
		}
	}
}

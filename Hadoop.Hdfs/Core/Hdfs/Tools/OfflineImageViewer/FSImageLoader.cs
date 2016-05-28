using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// FSImageLoader loads fsimage and provide methods to return JSON formatted
	/// file status of the namespace of the fsimage.
	/// </summary>
	internal class FSImageLoader
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FSImageHandler));

		private readonly string[] stringTable;

		private readonly byte[][] inodes;

		private readonly IDictionary<long, long[]> dirmap;

		private sealed class _IComparer_72 : IComparer<byte[]>
		{
			public _IComparer_72()
			{
			}

			// byte representation of inodes, sorted by id
			public int Compare(byte[] o1, byte[] o2)
			{
				try
				{
					FsImageProto.INodeSection.INode l = FsImageProto.INodeSection.INode.ParseFrom(o1);
					FsImageProto.INodeSection.INode r = FsImageProto.INodeSection.INode.ParseFrom(o2);
					if (l.GetId() < r.GetId())
					{
						return -1;
					}
					else
					{
						if (l.GetId() > r.GetId())
						{
							return 1;
						}
						else
						{
							return 0;
						}
					}
				}
				catch (InvalidProtocolBufferException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		private static readonly IComparer<byte[]> InodeBytesComparator = new _IComparer_72
			();

		private FSImageLoader(string[] stringTable, byte[][] inodes, IDictionary<long, long
			[]> dirmap)
		{
			this.stringTable = stringTable;
			this.inodes = inodes;
			this.dirmap = dirmap;
		}

		/// <summary>Load fsimage into the memory.</summary>
		/// <param name="inputFile">the filepath of the fsimage to load.</param>
		/// <returns>FSImageLoader</returns>
		/// <exception cref="System.IO.IOException">if failed to load fsimage.</exception>
		internal static Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.FSImageLoader Load
			(string inputFile)
		{
			Configuration conf = new Configuration();
			RandomAccessFile file = new RandomAccessFile(inputFile, "r");
			if (!FSImageUtil.CheckFileFormat(file))
			{
				throw new IOException("Unrecognized FSImage");
			}
			FsImageProto.FileSummary summary = FSImageUtil.LoadSummary(file);
			using (FileInputStream fin = new FileInputStream(file.GetFD()))
			{
				// Map to record INodeReference to the referred id
				ImmutableList<long> refIdList = null;
				string[] stringTable = null;
				byte[][] inodes = null;
				IDictionary<long, long[]> dirmap = null;
				AList<FsImageProto.FileSummary.Section> sections = Lists.NewArrayList(summary.GetSectionsList
					());
				sections.Sort(new _IComparer_126());
				foreach (FsImageProto.FileSummary.Section s in sections)
				{
					fin.GetChannel().Position(s.GetOffset());
					InputStream @is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec
						(), new BufferedInputStream(new LimitInputStream(fin, s.GetLength())));
					Log.Debug("Loading section " + s.GetName() + " length: " + s.GetLength());
					switch (FSImageFormatProtobuf.SectionName.FromString(s.GetName()))
					{
						case FSImageFormatProtobuf.SectionName.StringTable:
						{
							stringTable = LoadStringTable(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.Inode:
						{
							inodes = LoadINodeSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.InodeReference:
						{
							refIdList = LoadINodeReferenceSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.InodeDir:
						{
							dirmap = LoadINodeDirectorySection(@is, refIdList);
							break;
						}

						default:
						{
							break;
						}
					}
				}
				return new Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.FSImageLoader(stringTable
					, inodes, dirmap);
			}
		}

		private sealed class _IComparer_126 : IComparer<FsImageProto.FileSummary.Section>
		{
			public _IComparer_126()
			{
			}

			public int Compare(FsImageProto.FileSummary.Section s1, FsImageProto.FileSummary.Section
				 s2)
			{
				FSImageFormatProtobuf.SectionName n1 = FSImageFormatProtobuf.SectionName.FromString
					(s1.GetName());
				FSImageFormatProtobuf.SectionName n2 = FSImageFormatProtobuf.SectionName.FromString
					(s2.GetName());
				if (n1 == null)
				{
					return n2 == null ? 0 : -1;
				}
				else
				{
					if (n2 == null)
					{
						return -1;
					}
					else
					{
						return (int)(n1) - (int)(n2);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<long, long[]> LoadINodeDirectorySection(InputStream @in
			, IList<long> refIdList)
		{
			Log.Info("Loading inode directory section");
			IDictionary<long, long[]> dirs = Maps.NewHashMap();
			long counter = 0;
			while (true)
			{
				FsImageProto.INodeDirectorySection.DirEntry e = FsImageProto.INodeDirectorySection.DirEntry
					.ParseDelimitedFrom(@in);
				// note that in is a LimitedInputStream
				if (e == null)
				{
					break;
				}
				++counter;
				long[] l = new long[e.GetChildrenCount() + e.GetRefChildrenCount()];
				for (int i = 0; i < e.GetChildrenCount(); ++i)
				{
					l[i] = e.GetChildren(i);
				}
				for (int i_1 = e.GetChildrenCount(); i_1 < l.Length; i_1++)
				{
					int refId = e.GetRefChildren(i_1 - e.GetChildrenCount());
					l[i_1] = refIdList[refId];
				}
				dirs[e.GetParent()] = l;
			}
			Log.Info("Loaded " + counter + " directories");
			return dirs;
		}

		/// <exception cref="System.IO.IOException"/>
		private static ImmutableList<long> LoadINodeReferenceSection(InputStream @in)
		{
			Log.Info("Loading inode references");
			ImmutableList.Builder<long> builder = ImmutableList.Builder();
			long counter = 0;
			while (true)
			{
				FsImageProto.INodeReferenceSection.INodeReference e = FsImageProto.INodeReferenceSection.INodeReference
					.ParseDelimitedFrom(@in);
				if (e == null)
				{
					break;
				}
				++counter;
				builder.Add(e.GetReferredId());
			}
			Log.Info("Loaded " + counter + " inode references");
			return ((ImmutableList<long>)builder.Build());
		}

		/// <exception cref="System.IO.IOException"/>
		private static byte[][] LoadINodeSection(InputStream @in)
		{
			FsImageProto.INodeSection s = FsImageProto.INodeSection.ParseDelimitedFrom(@in);
			Log.Info("Loading " + s.GetNumInodes() + " inodes.");
			byte[][] inodes = new byte[(int)s.GetNumInodes()][];
			for (int i = 0; i < s.GetNumInodes(); ++i)
			{
				int size = CodedInputStream.ReadRawVarint32(@in.Read(), @in);
				byte[] bytes = new byte[size];
				IOUtils.ReadFully(@in, bytes, 0, size);
				inodes[i] = bytes;
			}
			Log.Debug("Sorting inodes");
			Arrays.Sort(inodes, InodeBytesComparator);
			Log.Debug("Finished sorting inodes");
			return inodes;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string[] LoadStringTable(InputStream @in)
		{
			FsImageProto.StringTableSection s = FsImageProto.StringTableSection.ParseDelimitedFrom
				(@in);
			Log.Info("Loading " + s.GetNumEntry() + " strings");
			string[] stringTable = new string[s.GetNumEntry() + 1];
			for (int i = 0; i < s.GetNumEntry(); ++i)
			{
				FsImageProto.StringTableSection.Entry e = FsImageProto.StringTableSection.Entry.ParseDelimitedFrom
					(@in);
				stringTable[e.GetId()] = e.GetStr();
			}
			return stringTable;
		}

		/// <summary>Return the JSON formatted FileStatus of the specified file.</summary>
		/// <param name="path">a path specifies a file</param>
		/// <returns>JSON formatted FileStatus</returns>
		/// <exception cref="System.IO.IOException">if failed to serialize fileStatus to JSON.
		/// 	</exception>
		internal virtual string GetFileStatus(string path)
		{
			ObjectMapper mapper = new ObjectMapper();
			FsImageProto.INodeSection.INode inode = FromINodeId(Lookup(path));
			return "{\"FileStatus\":\n" + mapper.WriteValueAsString(GetFileStatus(inode, false
				)) + "\n}\n";
		}

		/// <summary>Return the JSON formatted list of the files in the specified directory.</summary>
		/// <param name="path">a path specifies a directory to list</param>
		/// <returns>JSON formatted file list in the directory</returns>
		/// <exception cref="System.IO.IOException">if failed to serialize fileStatus to JSON.
		/// 	</exception>
		internal virtual string ListStatus(string path)
		{
			StringBuilder sb = new StringBuilder();
			ObjectMapper mapper = new ObjectMapper();
			IList<IDictionary<string, object>> fileStatusList = GetFileStatusList(path);
			sb.Append("{\"FileStatuses\":{\"FileStatus\":[\n");
			int i = 0;
			foreach (IDictionary<string, object> fileStatusMap in fileStatusList)
			{
				if (i++ != 0)
				{
					sb.Append(',');
				}
				sb.Append(mapper.WriteValueAsString(fileStatusMap));
			}
			sb.Append("\n]}}\n");
			return sb.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<IDictionary<string, object>> GetFileStatusList(string path)
		{
			IList<IDictionary<string, object>> list = new AList<IDictionary<string, object>>(
				);
			long id = Lookup(path);
			FsImageProto.INodeSection.INode inode = FromINodeId(id);
			if (inode.GetType() == FsImageProto.INodeSection.INode.Type.Directory)
			{
				if (!dirmap.Contains(id))
				{
					// if the directory is empty, return empty list
					return list;
				}
				long[] children = dirmap[id];
				foreach (long cid in children)
				{
					list.AddItem(GetFileStatus(FromINodeId(cid), true));
				}
			}
			else
			{
				list.AddItem(GetFileStatus(inode, false));
			}
			return list;
		}

		/// <summary>Return the JSON formatted ACL status of the specified file.</summary>
		/// <param name="path">a path specifies a file</param>
		/// <returns>JSON formatted AclStatus</returns>
		/// <exception cref="System.IO.IOException">if failed to serialize fileStatus to JSON.
		/// 	</exception>
		internal virtual string GetAclStatus(string path)
		{
			PermissionStatus p = GetPermissionStatus(path);
			IList<AclEntry> aclEntryList = GetAclEntryList(path);
			FsPermission permission = p.GetPermission();
			AclStatus.Builder builder = new AclStatus.Builder();
			builder.Owner(p.GetUserName()).Group(p.GetGroupName()).AddEntries(aclEntryList).SetPermission
				(permission).StickyBit(permission.GetStickyBit());
			AclStatus aclStatus = builder.Build();
			return JsonUtil.ToJsonString(aclStatus);
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<AclEntry> GetAclEntryList(string path)
		{
			long id = Lookup(path);
			FsImageProto.INodeSection.INode inode = FromINodeId(id);
			switch (inode.GetType())
			{
				case FsImageProto.INodeSection.INode.Type.File:
				{
					FsImageProto.INodeSection.INodeFile f = inode.GetFile();
					return FSImageFormatPBINode.Loader.LoadAclEntries(f.GetAcl(), stringTable);
				}

				case FsImageProto.INodeSection.INode.Type.Directory:
				{
					FsImageProto.INodeSection.INodeDirectory d = inode.GetDirectory();
					return FSImageFormatPBINode.Loader.LoadAclEntries(d.GetAcl(), stringTable);
				}

				default:
				{
					return new AList<AclEntry>();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private PermissionStatus GetPermissionStatus(string path)
		{
			long id = Lookup(path);
			FsImageProto.INodeSection.INode inode = FromINodeId(id);
			switch (inode.GetType())
			{
				case FsImageProto.INodeSection.INode.Type.File:
				{
					FsImageProto.INodeSection.INodeFile f = inode.GetFile();
					return FSImageFormatPBINode.Loader.LoadPermission(f.GetPermission(), stringTable);
				}

				case FsImageProto.INodeSection.INode.Type.Directory:
				{
					FsImageProto.INodeSection.INodeDirectory d = inode.GetDirectory();
					return FSImageFormatPBINode.Loader.LoadPermission(d.GetPermission(), stringTable);
				}

				case FsImageProto.INodeSection.INode.Type.Symlink:
				{
					FsImageProto.INodeSection.INodeSymlink s = inode.GetSymlink();
					return FSImageFormatPBINode.Loader.LoadPermission(s.GetPermission(), stringTable);
				}

				default:
				{
					return null;
				}
			}
		}

		/// <summary>Return the INodeId of the specified path.</summary>
		/// <exception cref="System.IO.IOException"/>
		private long Lookup(string path)
		{
			Preconditions.CheckArgument(path.StartsWith("/"));
			long id = INodeId.RootInodeId;
			for (int offset = 0; offset < path.Length; offset = next)
			{
				next = path.IndexOf('/', offset + 1);
				if (next == -1)
				{
					next = path.Length;
				}
				if (offset + 1 > next)
				{
					break;
				}
				string component = Sharpen.Runtime.Substring(path, offset + 1, next);
				if (component.IsEmpty())
				{
					continue;
				}
				long[] children = dirmap[id];
				if (children == null)
				{
					throw new FileNotFoundException(path);
				}
				bool found = false;
				foreach (long cid in children)
				{
					FsImageProto.INodeSection.INode child = FromINodeId(cid);
					if (component.Equals(child.GetName().ToStringUtf8()))
					{
						found = true;
						id = child.GetId();
						break;
					}
				}
				if (!found)
				{
					throw new FileNotFoundException(path);
				}
			}
			return id;
		}

		private IDictionary<string, object> GetFileStatus(FsImageProto.INodeSection.INode
			 inode, bool printSuffix)
		{
			IDictionary<string, object> map = Maps.NewHashMap();
			switch (inode.GetType())
			{
				case FsImageProto.INodeSection.INode.Type.File:
				{
					FsImageProto.INodeSection.INodeFile f = inode.GetFile();
					PermissionStatus p = FSImageFormatPBINode.Loader.LoadPermission(f.GetPermission()
						, stringTable);
					map["accessTime"] = f.GetAccessTime();
					map["blockSize"] = f.GetPreferredBlockSize();
					map["group"] = p.GetGroupName();
					map["length"] = GetFileSize(f);
					map["modificationTime"] = f.GetModificationTime();
					map["owner"] = p.GetUserName();
					map["pathSuffix"] = printSuffix ? inode.GetName().ToStringUtf8() : string.Empty;
					map["permission"] = ToString(p.GetPermission());
					map["replication"] = f.GetReplication();
					map["type"] = inode.GetType();
					map["fileId"] = inode.GetId();
					map["childrenNum"] = 0;
					return map;
				}

				case FsImageProto.INodeSection.INode.Type.Directory:
				{
					FsImageProto.INodeSection.INodeDirectory d = inode.GetDirectory();
					PermissionStatus p = FSImageFormatPBINode.Loader.LoadPermission(d.GetPermission()
						, stringTable);
					map["accessTime"] = 0;
					map["blockSize"] = 0;
					map["group"] = p.GetGroupName();
					map["length"] = 0;
					map["modificationTime"] = d.GetModificationTime();
					map["owner"] = p.GetUserName();
					map["pathSuffix"] = printSuffix ? inode.GetName().ToStringUtf8() : string.Empty;
					map["permission"] = ToString(p.GetPermission());
					map["replication"] = 0;
					map["type"] = inode.GetType();
					map["fileId"] = inode.GetId();
					map["childrenNum"] = dirmap.Contains(inode.GetId()) ? dirmap[inode.GetId()].Length
						 : 0;
					return map;
				}

				case FsImageProto.INodeSection.INode.Type.Symlink:
				{
					FsImageProto.INodeSection.INodeSymlink d = inode.GetSymlink();
					PermissionStatus p = FSImageFormatPBINode.Loader.LoadPermission(d.GetPermission()
						, stringTable);
					map["accessTime"] = d.GetAccessTime();
					map["blockSize"] = 0;
					map["group"] = p.GetGroupName();
					map["length"] = 0;
					map["modificationTime"] = d.GetModificationTime();
					map["owner"] = p.GetUserName();
					map["pathSuffix"] = printSuffix ? inode.GetName().ToStringUtf8() : string.Empty;
					map["permission"] = ToString(p.GetPermission());
					map["replication"] = 0;
					map["type"] = inode.GetType();
					map["symlink"] = d.GetTarget().ToStringUtf8();
					map["fileId"] = inode.GetId();
					map["childrenNum"] = 0;
					return map;
				}

				default:
				{
					return null;
				}
			}
		}

		internal static long GetFileSize(FsImageProto.INodeSection.INodeFile f)
		{
			long size = 0;
			foreach (HdfsProtos.BlockProto p in f.GetBlocksList())
			{
				size += p.GetNumBytes();
			}
			return size;
		}

		private string ToString(FsPermission permission)
		{
			return string.Format("%o", permission.ToShort());
		}

		/// <exception cref="System.IO.IOException"/>
		private FsImageProto.INodeSection.INode FromINodeId(long id)
		{
			int l = 0;
			int r = inodes.Length;
			while (l < r)
			{
				int mid = l + (r - l) / 2;
				FsImageProto.INodeSection.INode n = FsImageProto.INodeSection.INode.ParseFrom(inodes
					[mid]);
				long nid = n.GetId();
				if (id > nid)
				{
					l = mid + 1;
				}
				else
				{
					if (id < nid)
					{
						r = mid;
					}
					else
					{
						return n;
					}
				}
			}
			return null;
		}
	}
}

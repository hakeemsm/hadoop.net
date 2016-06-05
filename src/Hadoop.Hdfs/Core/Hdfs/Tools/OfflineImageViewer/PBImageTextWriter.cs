using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Fusesource.Leveldbjni;
using Org.Iq80.Leveldb;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// This class reads the protobuf-based fsimage and generates text output
	/// for each inode to
	/// <see cref="@out"/>
	/// . The sub-class can override
	/// <see>getEntry()</see>
	/// to generate formatted string for each inode.
	/// Since protobuf-based fsimage does not guarantee the order of inodes and
	/// directories, PBImageTextWriter runs two-phase scans:
	/// <ol>
	/// <li>The first phase, PBImageTextWriter scans the INode sections to reads the
	/// filename of each directory. It also scans the INode_Dir sections to loads
	/// the relationships between a directory and its children. It uses these metadata
	/// to build FS namespace and stored in
	/// <see cref="MetadataMap"/>
	/// </li>
	/// <li>The second phase, PBImageTextWriter re-scans the INode sections. For each
	/// inode, it looks up the path of the parent directory in the
	/// <see cref="MetadataMap"/>
	/// ,
	/// and generate output.</li>
	/// </ol>
	/// Two various of
	/// <see cref="MetadataMap"/>
	/// are provided.
	/// <see cref="InMemoryMetadataDB"/>
	/// stores all metadata in memory (O(n) memory) while
	/// <see cref="LevelDBMetadataMap"/>
	/// stores metadata in LevelDB on disk (O(1) memory).
	/// User can choose between them based on the time/space tradeoffs.
	/// </summary>
	internal abstract class PBImageTextWriter : IDisposable
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.PBImageTextWriter
			));

		/// <summary>
		/// This metadata map is used to construct the namespace before generating
		/// text outputs.
		/// </summary>
		/// <remarks>
		/// This metadata map is used to construct the namespace before generating
		/// text outputs.
		/// It contains two mapping relationships:
		/// <p>
		/// <li>It maps each inode (inode Id) to its parent directory (inode Id).</li>
		/// <li>It maps each directory from its inode Id.</li>
		/// </p>
		/// </remarks>
		private interface MetadataMap : IDisposable
		{
			/// <summary>Associate an inode with its parent directory.</summary>
			/// <exception cref="System.IO.IOException"/>
			void PutDirChild(long parentId, long childId);

			/// <summary>Associate a directory with its inode Id.</summary>
			/// <exception cref="System.IO.IOException"/>
			void PutDir(FsImageProto.INodeSection.INode dir);

			/// <summary>Get the full path of the parent directory for the given inode.</summary>
			/// <exception cref="System.IO.IOException"/>
			string GetParentPath(long inode);

			/// <summary>Synchronize metadata to persistent storage, if possible</summary>
			/// <exception cref="System.IO.IOException"/>
			void Sync();
		}

		/// <summary>Maintain all the metadata in memory.</summary>
		private class InMemoryMetadataDB : PBImageTextWriter.MetadataMap
		{
			/// <summary>Represent a directory in memory.</summary>
			private class Dir
			{
				private readonly long inode;

				private PBImageTextWriter.InMemoryMetadataDB.Dir parent = null;

				private string name;

				private string path = null;

				internal Dir(long inode, string name)
				{
					// cached full path of the directory.
					this.inode = inode;
					this.name = name;
				}

				private void SetParent(PBImageTextWriter.InMemoryMetadataDB.Dir parent)
				{
					Preconditions.CheckState(this.parent == null);
					this.parent = parent;
				}

				/// <summary>Returns the full path of this directory.</summary>
				private string GetPath()
				{
					if (this.parent == null)
					{
						return "/";
					}
					if (this.path == null)
					{
						this.path = new Path(parent.GetPath(), name.IsEmpty() ? "/" : name).ToString();
						this.name = null;
					}
					return this.path;
				}

				public override bool Equals(object o)
				{
					return o is PBImageTextWriter.InMemoryMetadataDB.Dir && inode == ((PBImageTextWriter.InMemoryMetadataDB.Dir
						)o).inode;
				}

				public override int GetHashCode()
				{
					return Sharpen.Extensions.ValueOf(inode).GetHashCode();
				}
			}

			/// <summary>INode Id to Dir object mapping</summary>
			private IDictionary<long, PBImageTextWriter.InMemoryMetadataDB.Dir> dirMap = new 
				Dictionary<long, PBImageTextWriter.InMemoryMetadataDB.Dir>();

			/// <summary>Children to parent directory INode ID mapping.</summary>
			private IDictionary<long, PBImageTextWriter.InMemoryMetadataDB.Dir> dirChildMap = 
				new Dictionary<long, PBImageTextWriter.InMemoryMetadataDB.Dir>();

			internal InMemoryMetadataDB()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			public virtual void PutDirChild(long parentId, long childId)
			{
				PBImageTextWriter.InMemoryMetadataDB.Dir parent = dirMap[parentId];
				PBImageTextWriter.InMemoryMetadataDB.Dir child = dirMap[childId];
				if (child != null)
				{
					child.SetParent(parent);
				}
				Preconditions.CheckState(!dirChildMap.Contains(childId));
				dirChildMap[childId] = parent;
			}

			public virtual void PutDir(FsImageProto.INodeSection.INode p)
			{
				Preconditions.CheckState(!dirMap.Contains(p.GetId()));
				PBImageTextWriter.InMemoryMetadataDB.Dir dir = new PBImageTextWriter.InMemoryMetadataDB.Dir
					(p.GetId(), p.GetName().ToStringUtf8());
				dirMap[p.GetId()] = dir;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetParentPath(long inode)
			{
				if (inode == INodeId.RootInodeId)
				{
					return string.Empty;
				}
				PBImageTextWriter.InMemoryMetadataDB.Dir parent = dirChildMap[inode];
				Preconditions.CheckState(parent != null, "Can not find parent directory for INode: %s"
					, inode);
				return parent.GetPath();
			}

			public virtual void Sync()
			{
			}
		}

		/// <summary>A MetadataMap that stores metadata in LevelDB.</summary>
		private class LevelDBMetadataMap : PBImageTextWriter.MetadataMap
		{
			/// <summary>Store metadata in LevelDB.</summary>
			private class LevelDBStore : IDisposable
			{
				private DB db = null;

				private WriteBatch batch = null;

				private int writeCount = 0;

				private const int BatchSize = 1024;

				/// <exception cref="System.IO.IOException"/>
				internal LevelDBStore(FilePath dbPath)
				{
					Options options = new Options();
					options.CreateIfMissing(true);
					options.ErrorIfExists(true);
					db = JniDBFactory.factory.Open(dbPath, options);
					batch = db.CreateWriteBatch();
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Close()
				{
					if (batch != null)
					{
						IOUtils.Cleanup(null, batch);
						batch = null;
					}
					IOUtils.Cleanup(null, db);
					db = null;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Put(byte[] key, byte[] value)
				{
					batch.Put(key, value);
					writeCount++;
					if (writeCount >= BatchSize)
					{
						Sync();
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual byte[] Get(byte[] key)
				{
					return db.Get(key);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Sync()
				{
					try
					{
						db.Write(batch);
					}
					finally
					{
						batch.Close();
						batch = null;
					}
					batch = db.CreateWriteBatch();
					writeCount = 0;
				}
			}

			/// <summary>A LRU cache for directory path strings.</summary>
			/// <remarks>
			/// A LRU cache for directory path strings.
			/// The key of this LRU cache is the inode of a directory.
			/// </remarks>
			[System.Serializable]
			private class DirPathCache : LinkedHashMap<long, string>
			{
				private const int Capacity = 16 * 1024;

				internal DirPathCache()
					: base(Capacity)
				{
				}

				protected override bool RemoveEldestEntry(KeyValuePair<long, string> entry)
				{
					return base.Count > Capacity;
				}
			}

			/// <summary>Map the child inode to the parent directory inode.</summary>
			private PBImageTextWriter.LevelDBMetadataMap.LevelDBStore dirChildMap = null;

			/// <summary>Directory entry map</summary>
			private PBImageTextWriter.LevelDBMetadataMap.LevelDBStore dirMap = null;

			private PBImageTextWriter.LevelDBMetadataMap.DirPathCache dirPathCache = new PBImageTextWriter.LevelDBMetadataMap.DirPathCache
				();

			/// <exception cref="System.IO.IOException"/>
			internal LevelDBMetadataMap(string baseDir)
			{
				FilePath dbDir = new FilePath(baseDir);
				if (dbDir.Exists())
				{
					FileUtils.DeleteDirectory(dbDir);
				}
				if (!dbDir.Mkdirs())
				{
					throw new IOException("Failed to mkdir on " + dbDir);
				}
				try
				{
					dirChildMap = new PBImageTextWriter.LevelDBMetadataMap.LevelDBStore(new FilePath(
						dbDir, "dirChildMap"));
					dirMap = new PBImageTextWriter.LevelDBMetadataMap.LevelDBStore(new FilePath(dbDir
						, "dirMap"));
				}
				catch (IOException e)
				{
					Log.Error("Failed to open LevelDBs", e);
					IOUtils.Cleanup(null, this);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				IOUtils.Cleanup(null, dirChildMap, dirMap);
				dirChildMap = null;
				dirMap = null;
			}

			private static byte[] ToBytes(long value)
			{
				return ((byte[])ByteBuffer.Allocate(8).PutLong(value).Array());
			}

			/// <exception cref="System.IO.UnsupportedEncodingException"/>
			private static byte[] ToBytes(string value)
			{
				return Sharpen.Runtime.GetBytesForString(value, "UTF-8");
			}

			private static long ToLong(byte[] bytes)
			{
				Preconditions.CheckArgument(bytes.Length == 8);
				return ByteBuffer.Wrap(bytes).GetLong();
			}

			/// <exception cref="System.IO.IOException"/>
			private static string ToString(byte[] bytes)
			{
				try
				{
					return Sharpen.Runtime.GetStringForBytes(bytes, "UTF-8");
				}
				catch (UnsupportedEncodingException e)
				{
					throw new IOException(e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void PutDirChild(long parentId, long childId)
			{
				dirChildMap.Put(ToBytes(childId), ToBytes(parentId));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void PutDir(FsImageProto.INodeSection.INode dir)
			{
				Preconditions.CheckArgument(dir.HasDirectory(), "INode %s (%s) is not a directory."
					, dir.GetId(), dir.GetName());
				dirMap.Put(ToBytes(dir.GetId()), ToBytes(dir.GetName().ToStringUtf8()));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetParentPath(long inode)
			{
				if (inode == INodeId.RootInodeId)
				{
					return "/";
				}
				byte[] bytes = dirChildMap.Get(ToBytes(inode));
				Preconditions.CheckState(bytes != null && bytes.Length == 8, "Can not find parent directory for inode %s, "
					 + "fsimage might be corrupted", inode);
				long parent = ToLong(bytes);
				if (!dirPathCache.Contains(parent))
				{
					bytes = dirMap.Get(ToBytes(parent));
					if (parent != INodeId.RootInodeId)
					{
						Preconditions.CheckState(bytes != null, "Can not find parent directory for inode %s, "
							 + ", the fsimage might be corrupted.", parent);
					}
					string parentName = ToString(bytes);
					string parentPath = new Path(GetParentPath(parent), parentName.IsEmpty() ? "/" : 
						parentName).ToString();
					dirPathCache[parent] = parentPath;
				}
				return dirPathCache[parent];
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Sync()
			{
				dirChildMap.Sync();
				dirMap.Sync();
			}
		}

		private string[] stringTable;

		private TextWriter @out;

		private PBImageTextWriter.MetadataMap metadataMap = null;

		/// <summary>Construct a PB FsImage writer to generate text file.</summary>
		/// <param name="out">the writer to output text information of fsimage.</param>
		/// <param name="tempPath">
		/// the path to store metadata. If it is empty, store metadata
		/// in memory instead.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		internal PBImageTextWriter(TextWriter @out, string tempPath)
		{
			this.@out = @out;
			if (tempPath.IsEmpty())
			{
				metadataMap = new PBImageTextWriter.InMemoryMetadataDB();
			}
			else
			{
				metadataMap = new PBImageTextWriter.LevelDBMetadataMap(tempPath);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			IOUtils.Cleanup(null, metadataMap);
		}

		/// <summary>Get text output for the given inode.</summary>
		/// <param name="parent">the path of parent directory</param>
		/// <param name="inode">the INode object to output.</param>
		protected internal abstract string GetEntry(string parent, FsImageProto.INodeSection.INode
			 inode);

		/// <exception cref="System.IO.IOException"/>
		public virtual void Visit(RandomAccessFile file)
		{
			Configuration conf = new Configuration();
			if (!FSImageUtil.CheckFileFormat(file))
			{
				throw new IOException("Unrecognized FSImage");
			}
			FsImageProto.FileSummary summary = FSImageUtil.LoadSummary(file);
			using (FileInputStream fin = new FileInputStream(file.GetFD()))
			{
				InputStream @is;
				AList<FsImageProto.FileSummary.Section> sections = Lists.NewArrayList(summary.GetSectionsList
					());
				sections.Sort(new _IComparer_427());
				foreach (FsImageProto.FileSummary.Section section in sections)
				{
					fin.GetChannel().Position(section.GetOffset());
					@is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec(), new BufferedInputStream
						(new LimitInputStream(fin, section.GetLength())));
					switch (FSImageFormatProtobuf.SectionName.FromString(section.GetName()))
					{
						case FSImageFormatProtobuf.SectionName.StringTable:
						{
							stringTable = FSImageLoader.LoadStringTable(@is);
							break;
						}

						default:
						{
							break;
						}
					}
				}
				LoadDirectories(fin, sections, summary, conf);
				LoadINodeDirSection(fin, sections, summary, conf);
				metadataMap.Sync();
				Output(conf, summary, fin, sections);
			}
		}

		private sealed class _IComparer_427 : IComparer<FsImageProto.FileSummary.Section>
		{
			public _IComparer_427()
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
		private void Output(Configuration conf, FsImageProto.FileSummary summary, FileInputStream
			 fin, AList<FsImageProto.FileSummary.Section> sections)
		{
			InputStream @is;
			long startTime = Time.MonotonicNow();
			foreach (FsImageProto.FileSummary.Section section in sections)
			{
				if (FSImageFormatProtobuf.SectionName.FromString(section.GetName()) == FSImageFormatProtobuf.SectionName
					.Inode)
				{
					fin.GetChannel().Position(section.GetOffset());
					@is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec(), new BufferedInputStream
						(new LimitInputStream(fin, section.GetLength())));
					OutputINodes(@is);
				}
			}
			long timeTaken = Time.MonotonicNow() - startTime;
			Log.Debug("Time to output inodes: {}ms", timeTaken);
		}

		protected internal virtual PermissionStatus GetPermission(long perm)
		{
			return FSImageFormatPBINode.Loader.LoadPermission(perm, stringTable);
		}

		/// <summary>Load the directories in the INode section.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void LoadDirectories(FileInputStream fin, IList<FsImageProto.FileSummary.Section
			> sections, FsImageProto.FileSummary summary, Configuration conf)
		{
			Log.Info("Loading directories");
			long startTime = Time.MonotonicNow();
			foreach (FsImageProto.FileSummary.Section section in sections)
			{
				if (FSImageFormatProtobuf.SectionName.FromString(section.GetName()) == FSImageFormatProtobuf.SectionName
					.Inode)
				{
					fin.GetChannel().Position(section.GetOffset());
					InputStream @is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec
						(), new BufferedInputStream(new LimitInputStream(fin, section.GetLength())));
					LoadDirectoriesInINodeSection(@is);
				}
			}
			long timeTaken = Time.MonotonicNow() - startTime;
			Log.Info("Finished loading directories in {}ms", timeTaken);
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadINodeDirSection(FileInputStream fin, IList<FsImageProto.FileSummary.Section
			> sections, FsImageProto.FileSummary summary, Configuration conf)
		{
			Log.Info("Loading INode directory section.");
			long startTime = Time.MonotonicNow();
			foreach (FsImageProto.FileSummary.Section section in sections)
			{
				if (FSImageFormatProtobuf.SectionName.FromString(section.GetName()) == FSImageFormatProtobuf.SectionName
					.InodeDir)
				{
					fin.GetChannel().Position(section.GetOffset());
					InputStream @is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec
						(), new BufferedInputStream(new LimitInputStream(fin, section.GetLength())));
					BuildNamespace(@is);
				}
			}
			long timeTaken = Time.MonotonicNow() - startTime;
			Log.Info("Finished loading INode directory section in {}ms", timeTaken);
		}

		/// <summary>Load the filenames of the directories from the INode section.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void LoadDirectoriesInINodeSection(InputStream @in)
		{
			FsImageProto.INodeSection s = FsImageProto.INodeSection.ParseDelimitedFrom(@in);
			Log.Info("Loading directories in INode section.");
			int numDirs = 0;
			for (int i = 0; i < s.GetNumInodes(); ++i)
			{
				FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.ParseDelimitedFrom
					(@in);
				if (Log.IsDebugEnabled() && i % 10000 == 0)
				{
					Log.Debug("Scanned {} inodes.", i);
				}
				if (p.HasDirectory())
				{
					metadataMap.PutDir(p);
					numDirs++;
				}
			}
			Log.Info("Found {} directories in INode section.", numDirs);
		}

		/// <summary>Scan the INodeDirectory section to construct the namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void BuildNamespace(InputStream @in)
		{
			int count = 0;
			while (true)
			{
				FsImageProto.INodeDirectorySection.DirEntry e = FsImageProto.INodeDirectorySection.DirEntry
					.ParseDelimitedFrom(@in);
				if (e == null)
				{
					break;
				}
				count++;
				if (Log.IsDebugEnabled() && count % 10000 == 0)
				{
					Log.Debug("Scanned {} directories.", count);
				}
				long parentId = e.GetParent();
				// Referred INode is not support for now.
				for (int i = 0; i < e.GetChildrenCount(); i++)
				{
					long childId = e.GetChildren(i);
					metadataMap.PutDirChild(parentId, childId);
				}
				Preconditions.CheckState(e.GetRefChildrenCount() == 0);
			}
			Log.Info("Scanned {} INode directories to build namespace.", count);
		}

		/// <exception cref="System.IO.IOException"/>
		private void OutputINodes(InputStream @in)
		{
			FsImageProto.INodeSection s = FsImageProto.INodeSection.ParseDelimitedFrom(@in);
			Log.Info("Found {} INodes in the INode section", s.GetNumInodes());
			for (int i = 0; i < s.GetNumInodes(); ++i)
			{
				FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.ParseDelimitedFrom
					(@in);
				string parentPath = metadataMap.GetParentPath(p.GetId());
				@out.WriteLine(GetEntry(parentPath, p));
				if (Log.IsDebugEnabled() && i % 100000 == 0)
				{
					Log.Debug("Outputted {} INodes.", i);
				}
			}
			Log.Info("Outputted {} INodes.", s.GetNumInodes());
		}
	}
}

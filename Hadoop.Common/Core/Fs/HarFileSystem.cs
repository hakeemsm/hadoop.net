using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// This is an implementation of the Hadoop Archive
	/// Filesystem.
	/// </summary>
	/// <remarks>
	/// This is an implementation of the Hadoop Archive
	/// Filesystem. This archive Filesystem has index files
	/// of the form _index* and has contents of the form
	/// part-*. The index files store the indexes of the
	/// real files. The index files are of the form _masterindex
	/// and _index. The master index is a level of indirection
	/// in to the index file to make the look ups faster. the index
	/// file is sorted with hash code of the paths that it contains
	/// and the master index contains pointers to the positions in
	/// index for ranges of hashcodes.
	/// </remarks>
	public class HarFileSystem : FileSystem
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.HarFileSystem
			));

		public const string MetadataCacheEntriesKey = "fs.har.metadatacache.entries";

		public const int MetadataCacheEntriesDefault = 10;

		public const int Version = 3;

		private static IDictionary<URI, HarFileSystem.HarMetaData> harMetaCache;

		private URI uri;

		private Path archivePath;

		private string harAuth;

		private HarFileSystem.HarMetaData metadata;

		private FileSystem fs;

		/// <summary>public construction of harfilesystem</summary>
		public HarFileSystem()
		{
		}

		// uri representation of this Har filesystem
		// the top level path of the archive
		// in the underlying file system
		// the har auth
		// pointer into the static metadata cache
		// Must call #initialize() method to set the underlying file system
		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>har</code></returns>
		public override string GetScheme()
		{
			return "har";
		}

		/// <summary>
		/// Constructor to create a HarFileSystem with an
		/// underlying filesystem.
		/// </summary>
		/// <param name="fs">underlying file system</param>
		public HarFileSystem(FileSystem fs)
		{
			this.fs = fs;
			this.statistics = fs.statistics;
		}

		private void InitializeMetadataCache(Configuration conf)
		{
			lock (this)
			{
				if (harMetaCache == null)
				{
					int cacheSize = conf.GetInt(MetadataCacheEntriesKey, MetadataCacheEntriesDefault);
					harMetaCache = Sharpen.Collections.SynchronizedMap(new HarFileSystem.LruCache<URI
						, HarFileSystem.HarMetaData>(cacheSize));
				}
			}
		}

		/// <summary>Initialize a Har filesystem per har archive.</summary>
		/// <remarks>
		/// Initialize a Har filesystem per har archive. The
		/// archive home directory is the top level directory
		/// in the filesystem that contains the HAR archive.
		/// Be careful with this method, you do not want to go
		/// on creating new Filesystem instances per call to
		/// path.getFileSystem().
		/// the uri of Har is
		/// har://underlyingfsscheme-host:port/archivepath.
		/// or
		/// har:///archivepath. This assumes the underlying filesystem
		/// to be used in case not specified.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI name, Configuration conf)
		{
			// initialize the metadata cache, if needed
			InitializeMetadataCache(conf);
			// decode the name
			URI underLyingURI = DecodeHarURI(name, conf);
			// we got the right har Path- now check if this is 
			// truly a har filesystem
			Path harPath = ArchivePath(new Path(name.GetScheme(), name.GetAuthority(), name.GetPath
				()));
			if (harPath == null)
			{
				throw new IOException("Invalid path for the Har Filesystem. " + name.ToString());
			}
			if (fs == null)
			{
				fs = FileSystem.Get(underLyingURI, conf);
			}
			uri = harPath.ToUri();
			archivePath = new Path(uri.GetPath());
			harAuth = GetHarAuth(underLyingURI);
			//check for the underlying fs containing
			// the index file
			Path masterIndexPath = new Path(archivePath, "_masterindex");
			Path archiveIndexPath = new Path(archivePath, "_index");
			if (!fs.Exists(masterIndexPath) || !fs.Exists(archiveIndexPath))
			{
				throw new IOException("Invalid path for the Har Filesystem. " + "No index file in "
					 + harPath);
			}
			metadata = harMetaCache[uri];
			if (metadata != null)
			{
				FileStatus mStat = fs.GetFileStatus(masterIndexPath);
				FileStatus aStat = fs.GetFileStatus(archiveIndexPath);
				if (mStat.GetModificationTime() != metadata.GetMasterIndexTimestamp() || aStat.GetModificationTime
					() != metadata.GetArchiveIndexTimestamp())
				{
					// the archive has been overwritten since we last read it
					// remove the entry from the meta data cache
					metadata = null;
					Sharpen.Collections.Remove(harMetaCache, uri);
				}
			}
			if (metadata == null)
			{
				metadata = new HarFileSystem.HarMetaData(this, fs, masterIndexPath, archiveIndexPath
					);
				metadata.ParseMetaData();
				harMetaCache[uri] = metadata;
			}
		}

		public override Configuration GetConf()
		{
			return fs.GetConf();
		}

		// get the version of the filesystem from the masterindex file
		// the version is currently not useful since its the first version
		// of archives
		/// <exception cref="System.IO.IOException"/>
		public virtual int GetHarVersion()
		{
			if (metadata != null)
			{
				return metadata.GetVersion();
			}
			else
			{
				throw new IOException("Invalid meta data for the Har Filesystem");
			}
		}

		/*
		* find the parent path that is the
		* archive path in the path. The last
		* path segment that ends with .har is
		* the path that will be returned.
		*/
		private Path ArchivePath(Path p)
		{
			Path retPath = null;
			Path tmp = p;
			for (int i = 0; i < p.Depth(); i++)
			{
				if (tmp.ToString().EndsWith(".har"))
				{
					retPath = tmp;
					break;
				}
				tmp = tmp.GetParent();
			}
			return retPath;
		}

		/// <summary>decode the raw URI to get the underlying URI</summary>
		/// <param name="rawURI">raw Har URI</param>
		/// <returns>filtered URI of the underlying fileSystem</returns>
		/// <exception cref="System.IO.IOException"/>
		private URI DecodeHarURI(URI rawURI, Configuration conf)
		{
			string tmpAuth = rawURI.GetAuthority();
			//we are using the default file
			//system in the config 
			//so create a underlying uri and 
			//return it
			if (tmpAuth == null)
			{
				//create a path 
				return FileSystem.GetDefaultUri(conf);
			}
			string authority = rawURI.GetAuthority();
			int i = authority.IndexOf('-');
			if (i < 0)
			{
				throw new IOException("URI: " + rawURI + " is an invalid Har URI since '-' not found."
					 + "  Expecting har://<scheme>-<host>/<path>.");
			}
			if (rawURI.GetQuery() != null)
			{
				// query component not allowed
				throw new IOException("query component in Path not supported  " + rawURI);
			}
			URI tmp;
			try
			{
				// convert <scheme>-<host> to <scheme>://<host>
				URI baseUri = new URI(authority.ReplaceFirst("-", "://"));
				tmp = new URI(baseUri.GetScheme(), baseUri.GetAuthority(), rawURI.GetPath(), rawURI
					.GetQuery(), rawURI.GetFragment());
			}
			catch (URISyntaxException)
			{
				throw new IOException("URI: " + rawURI + " is an invalid Har URI. Expecting har://<scheme>-<host>/<path>."
					);
			}
			return tmp;
		}

		/// <exception cref="System.IO.UnsupportedEncodingException"/>
		private static string DecodeString(string str)
		{
			return URLDecoder.Decode(str, "UTF-8");
		}

		/// <exception cref="System.IO.UnsupportedEncodingException"/>
		private string DecodeFileName(string fname)
		{
			int version = metadata.GetVersion();
			if (version == 2 || version == 3)
			{
				return DecodeString(fname);
			}
			return fname;
		}

		/// <summary>return the top level archive.</summary>
		public override Path GetWorkingDirectory()
		{
			return new Path(uri.ToString());
		}

		protected internal override Path GetInitialWorkingDirectory()
		{
			return GetWorkingDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetStatus(Path p)
		{
			return fs.GetStatus(p);
		}

		/// <summary>
		/// Create a har specific auth
		/// har-underlyingfs:port
		/// </summary>
		/// <param name="underLyingUri">
		/// the uri of underlying
		/// filesystem
		/// </param>
		/// <returns>har specific auth</returns>
		private string GetHarAuth(URI underLyingUri)
		{
			string auth = underLyingUri.GetScheme() + "-";
			if (underLyingUri.GetHost() != null)
			{
				if (underLyingUri.GetUserInfo() != null)
				{
					auth += underLyingUri.GetUserInfo();
					auth += "@";
				}
				auth += underLyingUri.GetHost();
				if (underLyingUri.GetPort() != -1)
				{
					auth += ":";
					auth += underLyingUri.GetPort();
				}
			}
			else
			{
				auth += ":";
			}
			return auth;
		}

		/// <summary>Used for delegation token related functionality.</summary>
		/// <remarks>
		/// Used for delegation token related functionality. Must delegate to
		/// underlying file system.
		/// </remarks>
		protected internal override URI GetCanonicalUri()
		{
			return fs.GetCanonicalUri();
		}

		protected internal override URI CanonicalizeUri(URI uri)
		{
			return fs.CanonicalizeUri(uri);
		}

		/// <summary>Returns the uri of this filesystem.</summary>
		/// <remarks>
		/// Returns the uri of this filesystem.
		/// The uri is of the form
		/// har://underlyingfsschema-host:port/pathintheunderlyingfs
		/// </remarks>
		public override URI GetUri()
		{
			return this.uri;
		}

		protected internal override void CheckPath(Path path)
		{
			fs.CheckPath(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path p)
		{
			return fs.ResolvePath(p);
		}

		/// <summary>
		/// this method returns the path
		/// inside the har filesystem.
		/// </summary>
		/// <remarks>
		/// this method returns the path
		/// inside the har filesystem.
		/// this is relative path inside
		/// the har filesystem.
		/// </remarks>
		/// <param name="path">the fully qualified path in the har filesystem.</param>
		/// <returns>relative path in the filesystem.</returns>
		private Path GetPathInHar(Path path)
		{
			Path harPath = new Path(path.ToUri().GetPath());
			if (archivePath.CompareTo(harPath) == 0)
			{
				return new Path(Path.Separator);
			}
			Path tmp = new Path(harPath.GetName());
			Path parent = harPath.GetParent();
			while (!(parent.CompareTo(archivePath) == 0))
			{
				if (parent.ToString().Equals(Path.Separator))
				{
					tmp = null;
					break;
				}
				tmp = new Path(parent.GetName(), tmp);
				parent = parent.GetParent();
			}
			if (tmp != null)
			{
				tmp = new Path(Path.Separator, tmp);
			}
			return tmp;
		}

		//the relative path of p. basically 
		// getting rid of /. Parsing and doing 
		// string manipulation is not good - so
		// just use the path api to do it.
		private Path MakeRelative(string initial, Path p)
		{
			string scheme = this.uri.GetScheme();
			string authority = this.uri.GetAuthority();
			Path root = new Path(Path.Separator);
			if (root.CompareTo(p) == 0)
			{
				return new Path(scheme, authority, initial);
			}
			Path retPath = new Path(p.GetName());
			Path parent = p.GetParent();
			for (int i = 0; i < p.Depth() - 1; i++)
			{
				retPath = new Path(parent.GetName(), retPath);
				parent = parent.GetParent();
			}
			return new Path(new Path(scheme, authority, initial), retPath.ToString());
		}

		/* this makes a path qualified in the har filesystem
		* (non-Javadoc)
		* @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
		* org.apache.hadoop.fs.Path)
		*/
		public override Path MakeQualified(Path path)
		{
			// make sure that we just get the 
			// path component 
			Path fsPath = path;
			if (!path.IsAbsolute())
			{
				fsPath = new Path(archivePath, path);
			}
			URI tmpURI = fsPath.ToUri();
			//change this to Har uri 
			return new Path(uri.GetScheme(), harAuth, tmpURI.GetPath());
		}

		/// <summary>Fix offset and length of block locations.</summary>
		/// <remarks>
		/// Fix offset and length of block locations.
		/// Note that this method modifies the original array.
		/// </remarks>
		/// <param name="locations">block locations of har part file</param>
		/// <param name="start">the start of the desired range in the contained file</param>
		/// <param name="len">the length of the desired range</param>
		/// <param name="fileOffsetInHar">the offset of the desired file in the har part file
		/// 	</param>
		/// <returns>block locations with fixed offset and length</returns>
		internal static BlockLocation[] FixBlockLocations(BlockLocation[] locations, long
			 start, long len, long fileOffsetInHar)
		{
			// offset 1 past last byte of desired range
			long end = start + len;
			foreach (BlockLocation location in locations)
			{
				// offset of part block relative to beginning of desired file
				// (may be negative if file starts in this part block)
				long harBlockStart = location.GetOffset() - fileOffsetInHar;
				// offset 1 past last byte of har block relative to beginning of
				// desired file
				long harBlockEnd = harBlockStart + location.GetLength();
				if (start > harBlockStart)
				{
					// desired range starts after beginning of this har block
					// fix offset to beginning of relevant range (relative to desired file)
					location.SetOffset(start);
					// fix length to relevant portion of har block
					location.SetLength(location.GetLength() - (start - harBlockStart));
				}
				else
				{
					// desired range includes beginning of this har block
					location.SetOffset(harBlockStart);
				}
				if (harBlockEnd > end)
				{
					// range ends before end of this har block
					// fix length to remove irrelevant portion at the end
					location.SetLength(location.GetLength() - (harBlockEnd - end));
				}
			}
			return locations;
		}

		/// <summary>
		/// Get block locations from the underlying fs and fix their
		/// offsets and lengths.
		/// </summary>
		/// <param name="file">the input file status to get block locations</param>
		/// <param name="start">the start of the desired range in the contained file</param>
		/// <param name="len">the length of the desired range</param>
		/// <returns>block locations for this segment of file</returns>
		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(FileStatus file, long start
			, long len)
		{
			HarFileSystem.HarStatus hstatus = GetFileHarStatus(file.GetPath());
			Path partPath = new Path(archivePath, hstatus.GetPartName());
			FileStatus partStatus = metadata.GetPartFileStatus(partPath);
			// get all part blocks that overlap with the desired file blocks
			BlockLocation[] locations = fs.GetFileBlockLocations(partStatus, hstatus.GetStartIndex
				() + start, len);
			return FixBlockLocations(locations, start, len, hstatus.GetStartIndex());
		}

		/// <summary>the hash of the path p inside  the filesystem</summary>
		/// <param name="p">the path in the harfilesystem</param>
		/// <returns>the hash code of the path.</returns>
		public static int GetHarHash(Path p)
		{
			return (p.ToString().GetHashCode() & unchecked((int)(0x7fffffff)));
		}

		internal class Store
		{
			public Store(long begin, long end)
			{
				this.begin = begin;
				this.end = end;
			}

			public long begin;

			public long end;
		}

		/// <summary>Get filestatuses of all the children of a given directory.</summary>
		/// <remarks>
		/// Get filestatuses of all the children of a given directory. This just reads
		/// through index file and reads line by line to get all statuses for children
		/// of a directory. Its a brute force way of getting all such filestatuses
		/// </remarks>
		/// <param name="parent">the parent path directory</param>
		/// <param name="statuses">the list to add the children filestatuses to</param>
		/// <exception cref="System.IO.IOException"/>
		private void FileStatusesInIndex(HarFileSystem.HarStatus parent, IList<FileStatus
			> statuses)
		{
			string parentString = parent.GetName();
			if (!parentString.EndsWith(Path.Separator))
			{
				parentString += Path.Separator;
			}
			Path harPath = new Path(parentString);
			int harlen = harPath.Depth();
			IDictionary<string, FileStatus> cache = new SortedDictionary<string, FileStatus>(
				);
			foreach (HarFileSystem.HarStatus hstatus in metadata.archive.Values)
			{
				string child = hstatus.GetName();
				if ((child.StartsWith(parentString)))
				{
					Path thisPath = new Path(child);
					if (thisPath.Depth() == harlen + 1)
					{
						statuses.AddItem(ToFileStatus(hstatus, cache));
					}
				}
			}
		}

		/// <summary>Combine the status stored in the index and the underlying status.</summary>
		/// <param name="h">status stored in the index</param>
		/// <param name="cache">caching the underlying file statuses</param>
		/// <returns>the combined file status</returns>
		/// <exception cref="System.IO.IOException"/>
		private FileStatus ToFileStatus(HarFileSystem.HarStatus h, IDictionary<string, FileStatus
			> cache)
		{
			FileStatus underlying = null;
			if (cache != null)
			{
				underlying = cache[h.partName];
			}
			if (underlying == null)
			{
				Path p = h.isDir ? archivePath : new Path(archivePath, h.partName);
				underlying = fs.GetFileStatus(p);
				if (cache != null)
				{
					cache[h.partName] = underlying;
				}
			}
			long modTime = 0;
			int version = metadata.GetVersion();
			if (version < 3)
			{
				modTime = underlying.GetModificationTime();
			}
			else
			{
				if (version == 3)
				{
					modTime = h.GetModificationTime();
				}
			}
			return new FileStatus(h.IsDir() ? 0L : h.GetLength(), h.IsDir(), underlying.GetReplication
				(), underlying.GetBlockSize(), modTime, underlying.GetAccessTime(), underlying.GetPermission
				(), underlying.GetOwner(), underlying.GetGroup(), MakeRelative(this.uri.GetPath(
				), new Path(h.name)));
		}

		private class HarStatus
		{
			internal bool isDir;

			internal string name;

			internal IList<string> children;

			internal string partName;

			internal long startIndex;

			internal long length;

			internal long modificationTime = 0;

			/// <exception cref="System.IO.UnsupportedEncodingException"/>
			public HarStatus(HarFileSystem _enclosing, string harString)
			{
				this._enclosing = _enclosing;
				// a single line parser for hadoop archives status 
				// stored in a single line in the index files 
				// the format is of the form 
				// filename "dir"/"file" partFileName startIndex length 
				// <space separated children>
				string[] splits = harString.Split(" ");
				this.name = this._enclosing.DecodeFileName(splits[0]);
				this.isDir = "dir".Equals(splits[1]);
				// this is equal to "none" if its a directory
				this.partName = splits[2];
				this.startIndex = long.Parse(splits[3]);
				this.length = long.Parse(splits[4]);
				int version = this._enclosing.metadata.GetVersion();
				string[] propSplits = null;
				// propSplits is used to retrieve the metainformation that Har versions
				// 1 & 2 missed (modification time, permission, owner group).
				// These fields are stored in an encoded string placed in different
				// locations depending on whether it's a file or directory entry.
				// If it's a directory, the string will be placed at the partName
				// location (directories have no partName because they don't have data
				// to be stored). This is done because the number of fields in a
				// directory entry is unbounded (all children are listed at the end)
				// If it's a file, the string will be the last field.
				if (this.isDir)
				{
					if (version == 3)
					{
						propSplits = HarFileSystem.DecodeString(this.partName).Split(" ");
					}
					this.children = new AList<string>();
					for (int i = 5; i < splits.Length; i++)
					{
						this.children.AddItem(this._enclosing.DecodeFileName(splits[i]));
					}
				}
				else
				{
					if (version == 3)
					{
						propSplits = HarFileSystem.DecodeString(splits[5]).Split(" ");
					}
				}
				if (propSplits != null && propSplits.Length >= 4)
				{
					this.modificationTime = long.Parse(propSplits[0]);
				}
			}

			// the fields below are stored in the file but are currently not used
			// by HarFileSystem
			// permission = new FsPermission(Short.parseShort(propSplits[1]));
			// owner = decodeString(propSplits[2]);
			// group = decodeString(propSplits[3]);
			public virtual bool IsDir()
			{
				return this.isDir;
			}

			public virtual string GetName()
			{
				return this.name;
			}

			public virtual string GetPartName()
			{
				return this.partName;
			}

			public virtual long GetStartIndex()
			{
				return this.startIndex;
			}

			public virtual long GetLength()
			{
				return this.length;
			}

			public virtual long GetModificationTime()
			{
				return this.modificationTime;
			}

			private readonly HarFileSystem _enclosing;
		}

		/// <summary>return the filestatus of files in har archive.</summary>
		/// <remarks>
		/// return the filestatus of files in har archive.
		/// The permission returned are that of the archive
		/// index files. The permissions are not persisted
		/// while creating a hadoop archive.
		/// </remarks>
		/// <param name="f">the path in har filesystem</param>
		/// <returns>filestatus.</returns>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			HarFileSystem.HarStatus hstatus = GetFileHarStatus(f);
			return ToFileStatus(hstatus, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private HarFileSystem.HarStatus GetFileHarStatus(Path f)
		{
			// get the fs DataInputStream for the underlying file
			// look up the index.
			Path p = MakeQualified(f);
			Path harPath = GetPathInHar(p);
			if (harPath == null)
			{
				throw new IOException("Invalid file name: " + f + " in " + uri);
			}
			HarFileSystem.HarStatus hstatus = metadata.archive[harPath];
			if (hstatus == null)
			{
				throw new FileNotFoundException("File: " + f + " does not exist in " + uri);
			}
			return hstatus;
		}

		/// <returns>null since no checksum algorithm is implemented.</returns>
		public override FileChecksum GetFileChecksum(Path f, long length)
		{
			return null;
		}

		/// <summary>
		/// Returns a har input stream which fakes end of
		/// file.
		/// </summary>
		/// <remarks>
		/// Returns a har input stream which fakes end of
		/// file. It reads the index files to get the part
		/// file name and the size and start of the file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			// get the fs DataInputStream for the underlying file
			HarFileSystem.HarStatus hstatus = GetFileHarStatus(f);
			if (hstatus.IsDir())
			{
				throw new FileNotFoundException(f + " : not a file in " + archivePath);
			}
			return new HarFileSystem.HarFSDataInputStream(fs, new Path(archivePath, hstatus.GetPartName
				()), hstatus.GetStartIndex(), hstatus.GetLength(), bufferSize);
		}

		/// <summary>Used for delegation token related functionality.</summary>
		/// <remarks>
		/// Used for delegation token related functionality. Must delegate to
		/// underlying file system.
		/// </remarks>
		public override FileSystem[] GetChildFileSystems()
		{
			return new FileSystem[] { fs };
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			throw new IOException("Har: create not allowed.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateNonRecursive(Path f, bool overwrite, int
			 bufferSize, short replication, long blockSize, Progressable progress)
		{
			throw new IOException("Har: create not allowed.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			throw new IOException("Har: append not allowed.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			base.Close();
			if (fs != null)
			{
				try
				{
					fs.Close();
				}
				catch (IOException)
				{
				}
			}
		}

		//this might already be closed
		// ignore
		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path src, short replication)
		{
			throw new IOException("Har: setReplication not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			throw new IOException("Har: rename not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f)
		{
			throw new IOException("Har: append not allowed");
		}

		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			throw new IOException("Har: truncate not allowed");
		}

		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			throw new IOException("Har: delete not allowed");
		}

		/// <summary>
		/// liststatus returns the children of a directory
		/// after looking up the index files.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			//need to see if the file is an index in file
			//get the filestatus of the archive directory
			// we will create fake filestatuses to return
			// to the client
			IList<FileStatus> statuses = new AList<FileStatus>();
			Path tmpPath = MakeQualified(f);
			Path harPath = GetPathInHar(tmpPath);
			HarFileSystem.HarStatus hstatus = metadata.archive[harPath];
			if (hstatus == null)
			{
				throw new FileNotFoundException("File " + f + " not found in " + archivePath);
			}
			if (hstatus.IsDir())
			{
				FileStatusesInIndex(hstatus, statuses);
			}
			else
			{
				statuses.AddItem(ToFileStatus(hstatus, null));
			}
			return Sharpen.Collections.ToArray(statuses, new FileStatus[statuses.Count]);
		}

		/// <summary>return the top level archive path.</summary>
		public override Path GetHomeDirectory()
		{
			return new Path(uri.ToString());
		}

		public override void SetWorkingDirectory(Path newDir)
		{
		}

		//does nothing.
		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			throw new IOException("Har: mkdirs not allowed");
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, bool overwrite, Path src, Path
			 dst)
		{
			throw new IOException("Har: copyfromlocalfile not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, bool overwrite, Path[] srcs, 
			Path dst)
		{
			throw new IOException("Har: copyfromlocalfile not allowed");
		}

		/// <summary>copies the file in the har filesystem to a local file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyToLocalFile(bool delSrc, Path src, Path dst)
		{
			FileUtil.Copy(this, src, GetLocal(GetConf()), dst, false, GetConf());
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override Path StartLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			throw new IOException("Har: startLocalOutput not allowed");
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void CompleteLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			throw new IOException("Har: completeLocalOutput not allowed");
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path p, string username, string groupname)
		{
			throw new IOException("Har: setowner not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path p, long mtime, long atime)
		{
			throw new IOException("Har: setTimes not allowed");
		}

		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path p, FsPermission permission)
		{
			throw new IOException("Har: setPermission not allowed");
		}

		/// <summary>Hadoop archives input stream.</summary>
		/// <remarks>
		/// Hadoop archives input stream. This input stream fakes EOF
		/// since archive files are part of bigger part files.
		/// </remarks>
		private class HarFSDataInputStream : FSDataInputStream
		{
			/// <summary>Create an input stream that fakes all the reads/positions/seeking.</summary>
			private class HarFsInputStream : FSInputStream, CanSetDropBehind, CanSetReadahead
			{
				private long position;

				private long start;

				private long end;

				private readonly FSDataInputStream underLyingStream;

				private readonly byte[] oneBytebuff = new byte[1];

				/// <exception cref="System.IO.IOException"/>
				internal HarFsInputStream(FileSystem fs, Path path, long start, long length, int 
					bufferSize)
				{
					//The underlying data input stream that the
					// underlying filesystem will return.
					//one byte buffer
					if (length < 0)
					{
						throw new ArgumentException("Negative length [" + length + "]");
					}
					underLyingStream = fs.Open(path, bufferSize);
					underLyingStream.Seek(start);
					// the start of this file in the part file
					this.start = start;
					// the position pointer in the part file
					this.position = start;
					// the end pointer in the part file
					this.end = start + length;
				}

				/// <exception cref="System.IO.IOException"/>
				public override int Available()
				{
					lock (this)
					{
						long remaining = end - underLyingStream.GetPos();
						if (remaining > int.MaxValue)
						{
							return int.MaxValue;
						}
						return (int)remaining;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
					lock (this)
					{
						underLyingStream.Close();
						base.Close();
					}
				}

				//not implemented
				public override void Mark(int readLimit)
				{
				}

				// do nothing 
				/// <summary>reset is not implemented</summary>
				/// <exception cref="System.IO.IOException"/>
				public override void Reset()
				{
					throw new IOException("reset not implemented.");
				}

				/// <exception cref="System.IO.IOException"/>
				public override int Read()
				{
					lock (this)
					{
						int ret = Read(oneBytebuff, 0, 1);
						return (ret <= 0) ? -1 : (oneBytebuff[0] & unchecked((int)(0xff)));
					}
				}

				// NB: currently this method actually never executed becusae
				// java.io.DataInputStream.read(byte[]) directly delegates to 
				// method java.io.InputStream.read(byte[], int, int).
				// However, potentially it can be invoked, so leave it intact for now.
				/// <exception cref="System.IO.IOException"/>
				public override int Read(byte[] b)
				{
					lock (this)
					{
						int ret = Read(b, 0, b.Length);
						return ret;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override int Read(byte[] b, int offset, int len)
				{
					lock (this)
					{
						int newlen = len;
						int ret = -1;
						if (position + len > end)
						{
							newlen = (int)(end - position);
						}
						// end case
						if (newlen == 0)
						{
							return ret;
						}
						ret = underLyingStream.Read(b, offset, newlen);
						position += ret;
						return ret;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override long Skip(long n)
				{
					lock (this)
					{
						long tmpN = n;
						if (tmpN > 0)
						{
							long actualRemaining = end - position;
							if (tmpN > actualRemaining)
							{
								tmpN = actualRemaining;
							}
							underLyingStream.Seek(tmpN + position);
							position += tmpN;
							return tmpN;
						}
						// NB: the contract is described in java.io.InputStream.skip(long):
						// this method returns the number of bytes actually skipped, so,
						// the return value should never be negative. 
						return 0;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override long GetPos()
				{
					lock (this)
					{
						return (position - start);
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Seek(long pos)
				{
					lock (this)
					{
						ValidatePosition(pos);
						position = start + pos;
						underLyingStream.Seek(position);
					}
				}

				/// <exception cref="System.IO.IOException"/>
				private void ValidatePosition(long pos)
				{
					if (pos < 0)
					{
						throw new IOException("Negative position: " + pos);
					}
					long length = end - start;
					if (pos > length)
					{
						throw new IOException("Position behind the end " + "of the stream (length = " + length
							 + "): " + pos);
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override bool SeekToNewSource(long targetPos)
				{
					// do not need to implement this
					// hdfs in itself does seektonewsource
					// while reading.
					return false;
				}

				/// <summary>implementing position readable.</summary>
				/// <exception cref="System.IO.IOException"/>
				public override int Read(long pos, byte[] b, int offset, int length)
				{
					int nlength = length;
					if (start + nlength + pos > end)
					{
						// length corrected to the real remaining length:
						nlength = (int)(end - start - pos);
					}
					if (nlength <= 0)
					{
						// EOS:
						return -1;
					}
					return underLyingStream.Read(pos + start, b, offset, nlength);
				}

				/// <summary>position readable again.</summary>
				/// <exception cref="System.IO.IOException"/>
				public override void ReadFully(long pos, byte[] b, int offset, int length)
				{
					if (start + length + pos > end)
					{
						throw new IOException("Not enough bytes to read.");
					}
					underLyingStream.ReadFully(pos + start, b, offset, length);
				}

				/// <exception cref="System.IO.IOException"/>
				public override void ReadFully(long pos, byte[] b)
				{
					ReadFully(pos, b, 0, b.Length);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void SetReadahead(long readahead)
				{
					underLyingStream.SetReadahead(readahead);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void SetDropBehind(bool dropBehind)
				{
					underLyingStream.SetDropBehind(dropBehind);
				}
			}

			/// <summary>constructors for har input stream.</summary>
			/// <param name="fs">the underlying filesystem</param>
			/// <param name="p">The path in the underlying filesystem</param>
			/// <param name="start">the start position in the part file</param>
			/// <param name="length">the length of valid data in the part file</param>
			/// <param name="bufsize">the buffer size</param>
			/// <exception cref="System.IO.IOException"/>
			public HarFSDataInputStream(FileSystem fs, Path p, long start, long length, int bufsize
				)
				: base(new HarFileSystem.HarFSDataInputStream.HarFsInputStream(fs, p, start, length
					, bufsize))
			{
			}
		}

		private class HarMetaData
		{
			private FileSystem fs;

			private int version;

			private Path masterIndexPath;

			private Path archiveIndexPath;

			private long masterIndexTimestamp;

			private long archiveIndexTimestamp;

			internal IList<HarFileSystem.Store> stores = new AList<HarFileSystem.Store>();

			internal IDictionary<Path, HarFileSystem.HarStatus> archive = new Dictionary<Path
				, HarFileSystem.HarStatus>();

			private IDictionary<Path, FileStatus> partFileStatuses = new Dictionary<Path, FileStatus
				>();

			public HarMetaData(HarFileSystem _enclosing, FileSystem fs, Path masterIndexPath, 
				Path archiveIndexPath)
			{
				this._enclosing = _enclosing;
				// the masterIndex of the archive
				// the index file 
				this.fs = fs;
				this.masterIndexPath = masterIndexPath;
				this.archiveIndexPath = archiveIndexPath;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileStatus GetPartFileStatus(Path partPath)
			{
				FileStatus status;
				status = this.partFileStatuses[partPath];
				if (status == null)
				{
					status = this.fs.GetFileStatus(partPath);
					this.partFileStatuses[partPath] = status;
				}
				return status;
			}

			public virtual long GetMasterIndexTimestamp()
			{
				return this.masterIndexTimestamp;
			}

			public virtual long GetArchiveIndexTimestamp()
			{
				return this.archiveIndexTimestamp;
			}

			private int GetVersion()
			{
				return this.version;
			}

			/// <exception cref="System.IO.IOException"/>
			private void ParseMetaData()
			{
				Text line = new Text();
				long read;
				FSDataInputStream @in = null;
				LineReader lin = null;
				try
				{
					@in = this.fs.Open(this.masterIndexPath);
					FileStatus masterStat = this.fs.GetFileStatus(this.masterIndexPath);
					this.masterIndexTimestamp = masterStat.GetModificationTime();
					lin = new LineReader(@in, this._enclosing.GetConf());
					read = lin.ReadLine(line);
					// the first line contains the version of the index file
					string versionLine = line.ToString();
					string[] arr = versionLine.Split(" ");
					this.version = System.Convert.ToInt32(arr[0]);
					// make it always backwards-compatible
					if (this.version > HarFileSystem.Version)
					{
						throw new IOException("Invalid version " + this.version + " expected " + HarFileSystem
							.Version);
					}
					// each line contains a hashcode range and the index file name
					string[] readStr;
					while (read < masterStat.GetLen())
					{
						int b = lin.ReadLine(line);
						read += b;
						readStr = line.ToString().Split(" ");
						this.stores.AddItem(new HarFileSystem.Store(long.Parse(readStr[2]), long.Parse(readStr
							[3])));
						line.Clear();
					}
				}
				catch (IOException ioe)
				{
					HarFileSystem.Log.Warn("Encountered exception ", ioe);
					throw;
				}
				finally
				{
					IOUtils.Cleanup(HarFileSystem.Log, lin, @in);
				}
				FSDataInputStream aIn = this.fs.Open(this.archiveIndexPath);
				try
				{
					FileStatus archiveStat = this.fs.GetFileStatus(this.archiveIndexPath);
					this.archiveIndexTimestamp = archiveStat.GetModificationTime();
					LineReader aLin;
					// now start reading the real index file
					foreach (HarFileSystem.Store s in this.stores)
					{
						read = 0;
						aIn.Seek(s.begin);
						aLin = new LineReader(aIn, this._enclosing.GetConf());
						while (read + s.begin < s.end)
						{
							int tmp = aLin.ReadLine(line);
							read += tmp;
							string lineFeed = line.ToString();
							string[] parsed = lineFeed.Split(" ");
							parsed[0] = this._enclosing.DecodeFileName(parsed[0]);
							this.archive[new Path(parsed[0])] = new HarFileSystem.HarStatus(this, lineFeed);
							line.Clear();
						}
					}
				}
				finally
				{
					IOUtils.Cleanup(HarFileSystem.Log, aIn);
				}
			}

			private readonly HarFileSystem _enclosing;
		}

		/*
		* testing purposes only:
		*/
		internal virtual HarFileSystem.HarMetaData GetMetadata()
		{
			return metadata;
		}

		[System.Serializable]
		private class LruCache<K, V> : LinkedHashMap<K, V>
		{
			private readonly int MaxEntries;

			public LruCache(int maxEntries)
				: base(maxEntries + 1, 1.0f, true)
			{
				MaxEntries = maxEntries;
			}

			protected override bool RemoveEldestEntry(KeyValuePair<K, V> eldest)
			{
				return Count > MaxEntries;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return fs.GetServerDefaults();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults(Path f)
		{
			return fs.GetServerDefaults(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetUsed()
		{
			return fs.GetUsed();
		}

		public override long GetDefaultBlockSize()
		{
			return fs.GetDefaultBlockSize();
		}

		public override long GetDefaultBlockSize(Path f)
		{
			return fs.GetDefaultBlockSize(f);
		}

		public override short GetDefaultReplication()
		{
			return fs.GetDefaultReplication();
		}

		public override short GetDefaultReplication(Path f)
		{
			return fs.GetDefaultReplication(f);
		}
	}
}

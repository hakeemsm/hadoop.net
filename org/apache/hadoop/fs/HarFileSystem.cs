using Sharpen;

namespace org.apache.hadoop.fs
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
	public class HarFileSystem : org.apache.hadoop.fs.FileSystem
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.HarFileSystem
			)));

		public const string METADATA_CACHE_ENTRIES_KEY = "fs.har.metadatacache.entries";

		public const int METADATA_CACHE_ENTRIES_DEFAULT = 10;

		public const int VERSION = 3;

		private static System.Collections.Generic.IDictionary<java.net.URI, org.apache.hadoop.fs.HarFileSystem.HarMetaData
			> harMetaCache;

		private java.net.URI uri;

		private org.apache.hadoop.fs.Path archivePath;

		private string harAuth;

		private org.apache.hadoop.fs.HarFileSystem.HarMetaData metadata;

		private org.apache.hadoop.fs.FileSystem fs;

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
		public override string getScheme()
		{
			return "har";
		}

		/// <summary>
		/// Constructor to create a HarFileSystem with an
		/// underlying filesystem.
		/// </summary>
		/// <param name="fs">underlying file system</param>
		public HarFileSystem(org.apache.hadoop.fs.FileSystem fs)
		{
			this.fs = fs;
			this.statistics = fs.statistics;
		}

		private void initializeMetadataCache(org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				if (harMetaCache == null)
				{
					int cacheSize = conf.getInt(METADATA_CACHE_ENTRIES_KEY, METADATA_CACHE_ENTRIES_DEFAULT
						);
					harMetaCache = java.util.Collections.synchronizedMap(new org.apache.hadoop.fs.HarFileSystem.LruCache
						<java.net.URI, org.apache.hadoop.fs.HarFileSystem.HarMetaData>(cacheSize));
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
		public override void initialize(java.net.URI name, org.apache.hadoop.conf.Configuration
			 conf)
		{
			// initialize the metadata cache, if needed
			initializeMetadataCache(conf);
			// decode the name
			java.net.URI underLyingURI = decodeHarURI(name, conf);
			// we got the right har Path- now check if this is 
			// truly a har filesystem
			org.apache.hadoop.fs.Path harPath = archivePath(new org.apache.hadoop.fs.Path(name
				.getScheme(), name.getAuthority(), name.getPath()));
			if (harPath == null)
			{
				throw new System.IO.IOException("Invalid path for the Har Filesystem. " + name.ToString
					());
			}
			if (fs == null)
			{
				fs = org.apache.hadoop.fs.FileSystem.get(underLyingURI, conf);
			}
			uri = harPath.toUri();
			archivePath = new org.apache.hadoop.fs.Path(uri.getPath());
			harAuth = getHarAuth(underLyingURI);
			//check for the underlying fs containing
			// the index file
			org.apache.hadoop.fs.Path masterIndexPath = new org.apache.hadoop.fs.Path(archivePath
				, "_masterindex");
			org.apache.hadoop.fs.Path archiveIndexPath = new org.apache.hadoop.fs.Path(archivePath
				, "_index");
			if (!fs.exists(masterIndexPath) || !fs.exists(archiveIndexPath))
			{
				throw new System.IO.IOException("Invalid path for the Har Filesystem. " + "No index file in "
					 + harPath);
			}
			metadata = harMetaCache[uri];
			if (metadata != null)
			{
				org.apache.hadoop.fs.FileStatus mStat = fs.getFileStatus(masterIndexPath);
				org.apache.hadoop.fs.FileStatus aStat = fs.getFileStatus(archiveIndexPath);
				if (mStat.getModificationTime() != metadata.getMasterIndexTimestamp() || aStat.getModificationTime
					() != metadata.getArchiveIndexTimestamp())
				{
					// the archive has been overwritten since we last read it
					// remove the entry from the meta data cache
					metadata = null;
					Sharpen.Collections.Remove(harMetaCache, uri);
				}
			}
			if (metadata == null)
			{
				metadata = new org.apache.hadoop.fs.HarFileSystem.HarMetaData(this, fs, masterIndexPath
					, archiveIndexPath);
				metadata.parseMetaData();
				harMetaCache[uri] = metadata;
			}
		}

		public override org.apache.hadoop.conf.Configuration getConf()
		{
			return fs.getConf();
		}

		// get the version of the filesystem from the masterindex file
		// the version is currently not useful since its the first version
		// of archives
		/// <exception cref="System.IO.IOException"/>
		public virtual int getHarVersion()
		{
			if (metadata != null)
			{
				return metadata.getVersion();
			}
			else
			{
				throw new System.IO.IOException("Invalid meta data for the Har Filesystem");
			}
		}

		/*
		* find the parent path that is the
		* archive path in the path. The last
		* path segment that ends with .har is
		* the path that will be returned.
		*/
		private org.apache.hadoop.fs.Path archivePath(org.apache.hadoop.fs.Path p)
		{
			org.apache.hadoop.fs.Path retPath = null;
			org.apache.hadoop.fs.Path tmp = p;
			for (int i = 0; i < p.depth(); i++)
			{
				if (tmp.ToString().EndsWith(".har"))
				{
					retPath = tmp;
					break;
				}
				tmp = tmp.getParent();
			}
			return retPath;
		}

		/// <summary>decode the raw URI to get the underlying URI</summary>
		/// <param name="rawURI">raw Har URI</param>
		/// <returns>filtered URI of the underlying fileSystem</returns>
		/// <exception cref="System.IO.IOException"/>
		private java.net.URI decodeHarURI(java.net.URI rawURI, org.apache.hadoop.conf.Configuration
			 conf)
		{
			string tmpAuth = rawURI.getAuthority();
			//we are using the default file
			//system in the config 
			//so create a underlying uri and 
			//return it
			if (tmpAuth == null)
			{
				//create a path 
				return org.apache.hadoop.fs.FileSystem.getDefaultUri(conf);
			}
			string authority = rawURI.getAuthority();
			int i = authority.IndexOf('-');
			if (i < 0)
			{
				throw new System.IO.IOException("URI: " + rawURI + " is an invalid Har URI since '-' not found."
					 + "  Expecting har://<scheme>-<host>/<path>.");
			}
			if (rawURI.getQuery() != null)
			{
				// query component not allowed
				throw new System.IO.IOException("query component in Path not supported  " + rawURI
					);
			}
			java.net.URI tmp;
			try
			{
				// convert <scheme>-<host> to <scheme>://<host>
				java.net.URI baseUri = new java.net.URI(authority.replaceFirst("-", "://"));
				tmp = new java.net.URI(baseUri.getScheme(), baseUri.getAuthority(), rawURI.getPath
					(), rawURI.getQuery(), rawURI.getFragment());
			}
			catch (java.net.URISyntaxException)
			{
				throw new System.IO.IOException("URI: " + rawURI + " is an invalid Har URI. Expecting har://<scheme>-<host>/<path>."
					);
			}
			return tmp;
		}

		/// <exception cref="java.io.UnsupportedEncodingException"/>
		private static string decodeString(string str)
		{
			return java.net.URLDecoder.decode(str, "UTF-8");
		}

		/// <exception cref="java.io.UnsupportedEncodingException"/>
		private string decodeFileName(string fname)
		{
			int version = metadata.getVersion();
			if (version == 2 || version == 3)
			{
				return decodeString(fname);
			}
			return fname;
		}

		/// <summary>return the top level archive.</summary>
		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return new org.apache.hadoop.fs.Path(uri.ToString());
		}

		protected internal override org.apache.hadoop.fs.Path getInitialWorkingDirectory(
			)
		{
			return getWorkingDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getStatus(org.apache.hadoop.fs.Path
			 p)
		{
			return fs.getStatus(p);
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
		private string getHarAuth(java.net.URI underLyingUri)
		{
			string auth = underLyingUri.getScheme() + "-";
			if (underLyingUri.getHost() != null)
			{
				if (underLyingUri.getUserInfo() != null)
				{
					auth += underLyingUri.getUserInfo();
					auth += "@";
				}
				auth += underLyingUri.getHost();
				if (underLyingUri.getPort() != -1)
				{
					auth += ":";
					auth += underLyingUri.getPort();
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
		protected internal override java.net.URI getCanonicalUri()
		{
			return fs.getCanonicalUri();
		}

		protected internal override java.net.URI canonicalizeUri(java.net.URI uri)
		{
			return fs.canonicalizeUri(uri);
		}

		/// <summary>Returns the uri of this filesystem.</summary>
		/// <remarks>
		/// Returns the uri of this filesystem.
		/// The uri is of the form
		/// har://underlyingfsschema-host:port/pathintheunderlyingfs
		/// </remarks>
		public override java.net.URI getUri()
		{
			return this.uri;
		}

		protected internal override void checkPath(org.apache.hadoop.fs.Path path)
		{
			fs.checkPath(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p
			)
		{
			return fs.resolvePath(p);
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
		private org.apache.hadoop.fs.Path getPathInHar(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.Path harPath = new org.apache.hadoop.fs.Path(path.toUri().getPath
				());
			if (archivePath.compareTo(harPath) == 0)
			{
				return new org.apache.hadoop.fs.Path(org.apache.hadoop.fs.Path.SEPARATOR);
			}
			org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(harPath.getName());
			org.apache.hadoop.fs.Path parent = harPath.getParent();
			while (!(parent.compareTo(archivePath) == 0))
			{
				if (parent.ToString().Equals(org.apache.hadoop.fs.Path.SEPARATOR))
				{
					tmp = null;
					break;
				}
				tmp = new org.apache.hadoop.fs.Path(parent.getName(), tmp);
				parent = parent.getParent();
			}
			if (tmp != null)
			{
				tmp = new org.apache.hadoop.fs.Path(org.apache.hadoop.fs.Path.SEPARATOR, tmp);
			}
			return tmp;
		}

		//the relative path of p. basically 
		// getting rid of /. Parsing and doing 
		// string manipulation is not good - so
		// just use the path api to do it.
		private org.apache.hadoop.fs.Path makeRelative(string initial, org.apache.hadoop.fs.Path
			 p)
		{
			string scheme = this.uri.getScheme();
			string authority = this.uri.getAuthority();
			org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(org.apache.hadoop.fs.Path
				.SEPARATOR);
			if (root.compareTo(p) == 0)
			{
				return new org.apache.hadoop.fs.Path(scheme, authority, initial);
			}
			org.apache.hadoop.fs.Path retPath = new org.apache.hadoop.fs.Path(p.getName());
			org.apache.hadoop.fs.Path parent = p.getParent();
			for (int i = 0; i < p.depth() - 1; i++)
			{
				retPath = new org.apache.hadoop.fs.Path(parent.getName(), retPath);
				parent = parent.getParent();
			}
			return new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path(scheme, authority
				, initial), retPath.ToString());
		}

		/* this makes a path qualified in the har filesystem
		* (non-Javadoc)
		* @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
		* org.apache.hadoop.fs.Path)
		*/
		public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 path)
		{
			// make sure that we just get the 
			// path component 
			org.apache.hadoop.fs.Path fsPath = path;
			if (!path.isAbsolute())
			{
				fsPath = new org.apache.hadoop.fs.Path(archivePath, path);
			}
			java.net.URI tmpURI = fsPath.toUri();
			//change this to Har uri 
			return new org.apache.hadoop.fs.Path(uri.getScheme(), harAuth, tmpURI.getPath());
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
		internal static org.apache.hadoop.fs.BlockLocation[] fixBlockLocations(org.apache.hadoop.fs.BlockLocation
			[] locations, long start, long len, long fileOffsetInHar)
		{
			// offset 1 past last byte of desired range
			long end = start + len;
			foreach (org.apache.hadoop.fs.BlockLocation location in locations)
			{
				// offset of part block relative to beginning of desired file
				// (may be negative if file starts in this part block)
				long harBlockStart = location.getOffset() - fileOffsetInHar;
				// offset 1 past last byte of har block relative to beginning of
				// desired file
				long harBlockEnd = harBlockStart + location.getLength();
				if (start > harBlockStart)
				{
					// desired range starts after beginning of this har block
					// fix offset to beginning of relevant range (relative to desired file)
					location.setOffset(start);
					// fix length to relevant portion of har block
					location.setLength(location.getLength() - (start - harBlockStart));
				}
				else
				{
					// desired range includes beginning of this har block
					location.setOffset(harBlockStart);
				}
				if (harBlockEnd > end)
				{
					// range ends before end of this har block
					// fix length to remove irrelevant portion at the end
					location.setLength(location.getLength() - (harBlockEnd - end));
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
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus
			 file, long start, long len)
		{
			org.apache.hadoop.fs.HarFileSystem.HarStatus hstatus = getFileHarStatus(file.getPath
				());
			org.apache.hadoop.fs.Path partPath = new org.apache.hadoop.fs.Path(archivePath, hstatus
				.getPartName());
			org.apache.hadoop.fs.FileStatus partStatus = metadata.getPartFileStatus(partPath);
			// get all part blocks that overlap with the desired file blocks
			org.apache.hadoop.fs.BlockLocation[] locations = fs.getFileBlockLocations(partStatus
				, hstatus.getStartIndex() + start, len);
			return fixBlockLocations(locations, start, len, hstatus.getStartIndex());
		}

		/// <summary>the hash of the path p inside  the filesystem</summary>
		/// <param name="p">the path in the harfilesystem</param>
		/// <returns>the hash code of the path.</returns>
		public static int getHarHash(org.apache.hadoop.fs.Path p)
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
		private void fileStatusesInIndex(org.apache.hadoop.fs.HarFileSystem.HarStatus parent
			, System.Collections.Generic.IList<org.apache.hadoop.fs.FileStatus> statuses)
		{
			string parentString = parent.getName();
			if (!parentString.EndsWith(org.apache.hadoop.fs.Path.SEPARATOR))
			{
				parentString += org.apache.hadoop.fs.Path.SEPARATOR;
			}
			org.apache.hadoop.fs.Path harPath = new org.apache.hadoop.fs.Path(parentString);
			int harlen = harPath.depth();
			System.Collections.Generic.IDictionary<string, org.apache.hadoop.fs.FileStatus> cache
				 = new System.Collections.Generic.SortedDictionary<string, org.apache.hadoop.fs.FileStatus
				>();
			foreach (org.apache.hadoop.fs.HarFileSystem.HarStatus hstatus in metadata.archive
				.Values)
			{
				string child = hstatus.getName();
				if ((child.StartsWith(parentString)))
				{
					org.apache.hadoop.fs.Path thisPath = new org.apache.hadoop.fs.Path(child);
					if (thisPath.depth() == harlen + 1)
					{
						statuses.add(toFileStatus(hstatus, cache));
					}
				}
			}
		}

		/// <summary>Combine the status stored in the index and the underlying status.</summary>
		/// <param name="h">status stored in the index</param>
		/// <param name="cache">caching the underlying file statuses</param>
		/// <returns>the combined file status</returns>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FileStatus toFileStatus(org.apache.hadoop.fs.HarFileSystem.HarStatus
			 h, System.Collections.Generic.IDictionary<string, org.apache.hadoop.fs.FileStatus
			> cache)
		{
			org.apache.hadoop.fs.FileStatus underlying = null;
			if (cache != null)
			{
				underlying = cache[h.partName];
			}
			if (underlying == null)
			{
				org.apache.hadoop.fs.Path p = h.isDir ? archivePath : new org.apache.hadoop.fs.Path
					(archivePath, h.partName);
				underlying = fs.getFileStatus(p);
				if (cache != null)
				{
					cache[h.partName] = underlying;
				}
			}
			long modTime = 0;
			int version = metadata.getVersion();
			if (version < 3)
			{
				modTime = underlying.getModificationTime();
			}
			else
			{
				if (version == 3)
				{
					modTime = h.getModificationTime();
				}
			}
			return new org.apache.hadoop.fs.FileStatus(h.isDir() ? 0L : h.getLength(), h.isDir
				(), underlying.getReplication(), underlying.getBlockSize(), modTime, underlying.
				getAccessTime(), underlying.getPermission(), underlying.getOwner(), underlying.getGroup
				(), makeRelative(this.uri.getPath(), new org.apache.hadoop.fs.Path(h.name)));
		}

		private class HarStatus
		{
			internal bool isDir;

			internal string name;

			internal System.Collections.Generic.IList<string> children;

			internal string partName;

			internal long startIndex;

			internal long length;

			internal long modificationTime = 0;

			/// <exception cref="java.io.UnsupportedEncodingException"/>
			public HarStatus(HarFileSystem _enclosing, string harString)
			{
				this._enclosing = _enclosing;
				// a single line parser for hadoop archives status 
				// stored in a single line in the index files 
				// the format is of the form 
				// filename "dir"/"file" partFileName startIndex length 
				// <space separated children>
				string[] splits = harString.split(" ");
				this.name = this._enclosing.decodeFileName(splits[0]);
				this.isDir = "dir".Equals(splits[1]);
				// this is equal to "none" if its a directory
				this.partName = splits[2];
				this.startIndex = long.Parse(splits[3]);
				this.length = long.Parse(splits[4]);
				int version = this._enclosing.metadata.getVersion();
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
						propSplits = org.apache.hadoop.fs.HarFileSystem.decodeString(this.partName).split
							(" ");
					}
					this.children = new System.Collections.Generic.List<string>();
					for (int i = 5; i < splits.Length; i++)
					{
						this.children.add(this._enclosing.decodeFileName(splits[i]));
					}
				}
				else
				{
					if (version == 3)
					{
						propSplits = org.apache.hadoop.fs.HarFileSystem.decodeString(splits[5]).split(" "
							);
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
			public virtual bool isDir()
			{
				return this.isDir;
			}

			public virtual string getName()
			{
				return this.name;
			}

			public virtual string getPartName()
			{
				return this.partName;
			}

			public virtual long getStartIndex()
			{
				return this.startIndex;
			}

			public virtual long getLength()
			{
				return this.length;
			}

			public virtual long getModificationTime()
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
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.HarFileSystem.HarStatus hstatus = getFileHarStatus(f);
			return toFileStatus(hstatus, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.HarFileSystem.HarStatus getFileHarStatus(org.apache.hadoop.fs.Path
			 f)
		{
			// get the fs DataInputStream for the underlying file
			// look up the index.
			org.apache.hadoop.fs.Path p = makeQualified(f);
			org.apache.hadoop.fs.Path harPath = getPathInHar(p);
			if (harPath == null)
			{
				throw new System.IO.IOException("Invalid file name: " + f + " in " + uri);
			}
			org.apache.hadoop.fs.HarFileSystem.HarStatus hstatus = metadata.archive[harPath];
			if (hstatus == null)
			{
				throw new java.io.FileNotFoundException("File: " + f + " does not exist in " + uri
					);
			}
			return hstatus;
		}

		/// <returns>null since no checksum algorithm is implemented.</returns>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f, long length)
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
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			// get the fs DataInputStream for the underlying file
			org.apache.hadoop.fs.HarFileSystem.HarStatus hstatus = getFileHarStatus(f);
			if (hstatus.isDir())
			{
				throw new java.io.FileNotFoundException(f + " : not a file in " + archivePath);
			}
			return new org.apache.hadoop.fs.HarFileSystem.HarFSDataInputStream(fs, new org.apache.hadoop.fs.Path
				(archivePath, hstatus.getPartName()), hstatus.getStartIndex(), hstatus.getLength
				(), bufferSize);
		}

		/// <summary>Used for delegation token related functionality.</summary>
		/// <remarks>
		/// Used for delegation token related functionality. Must delegate to
		/// underlying file system.
		/// </remarks>
		public override org.apache.hadoop.fs.FileSystem[] getChildFileSystems()
		{
			return new org.apache.hadoop.fs.FileSystem[] { fs };
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			throw new System.IO.IOException("Har: create not allowed.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			throw new System.IO.IOException("Har: create not allowed.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			throw new System.IO.IOException("Har: append not allowed.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			base.close();
			if (fs != null)
			{
				try
				{
					fs.close();
				}
				catch (System.IO.IOException)
				{
				}
			}
		}

		//this might already be closed
		// ignore
		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path src, short replication
			)
		{
			throw new System.IO.IOException("Har: setReplication not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			throw new System.IO.IOException("Har: rename not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f)
		{
			throw new System.IO.IOException("Har: append not allowed");
		}

		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			throw new System.IO.IOException("Har: truncate not allowed");
		}

		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			throw new System.IO.IOException("Har: delete not allowed");
		}

		/// <summary>
		/// liststatus returns the children of a directory
		/// after looking up the index files.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			//need to see if the file is an index in file
			//get the filestatus of the archive directory
			// we will create fake filestatuses to return
			// to the client
			System.Collections.Generic.IList<org.apache.hadoop.fs.FileStatus> statuses = new 
				System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus>();
			org.apache.hadoop.fs.Path tmpPath = makeQualified(f);
			org.apache.hadoop.fs.Path harPath = getPathInHar(tmpPath);
			org.apache.hadoop.fs.HarFileSystem.HarStatus hstatus = metadata.archive[harPath];
			if (hstatus == null)
			{
				throw new java.io.FileNotFoundException("File " + f + " not found in " + archivePath
					);
			}
			if (hstatus.isDir())
			{
				fileStatusesInIndex(hstatus, statuses);
			}
			else
			{
				statuses.add(toFileStatus(hstatus, null));
			}
			return Sharpen.Collections.ToArray(statuses, new org.apache.hadoop.fs.FileStatus[
				statuses.Count]);
		}

		/// <summary>return the top level archive path.</summary>
		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return new org.apache.hadoop.fs.Path(uri.ToString());
		}

		public override void setWorkingDirectory(org.apache.hadoop.fs.Path newDir)
		{
		}

		//does nothing.
		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			throw new System.IO.IOException("Har: mkdirs not allowed");
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, bool overwrite, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.Path dst)
		{
			throw new System.IO.IOException("Har: copyfromlocalfile not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, bool overwrite, org.apache.hadoop.fs.Path
			[] srcs, org.apache.hadoop.fs.Path dst)
		{
			throw new System.IO.IOException("Har: copyfromlocalfile not allowed");
		}

		/// <summary>copies the file in the har filesystem to a local file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, 
			org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.fs.FileUtil.copy(this, src, getLocal(getConf()), dst, false, getConf
				());
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path startLocalOutput(org.apache.hadoop.fs.Path
			 fsOutputFile, org.apache.hadoop.fs.Path tmpLocalFile)
		{
			throw new System.IO.IOException("Har: startLocalOutput not allowed");
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void completeLocalOutput(org.apache.hadoop.fs.Path fsOutputFile, 
			org.apache.hadoop.fs.Path tmpLocalFile)
		{
			throw new System.IO.IOException("Har: completeLocalOutput not allowed");
		}

		/// <summary>not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path p, string username, string
			 groupname)
		{
			throw new System.IO.IOException("Har: setowner not allowed");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path p, long mtime, long atime
			)
		{
			throw new System.IO.IOException("Har: setTimes not allowed");
		}

		/// <summary>Not implemented.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path p, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			throw new System.IO.IOException("Har: setPermission not allowed");
		}

		/// <summary>Hadoop archives input stream.</summary>
		/// <remarks>
		/// Hadoop archives input stream. This input stream fakes EOF
		/// since archive files are part of bigger part files.
		/// </remarks>
		private class HarFSDataInputStream : org.apache.hadoop.fs.FSDataInputStream
		{
			/// <summary>Create an input stream that fakes all the reads/positions/seeking.</summary>
			private class HarFsInputStream : org.apache.hadoop.fs.FSInputStream, org.apache.hadoop.fs.CanSetDropBehind
				, org.apache.hadoop.fs.CanSetReadahead
			{
				private long position;

				private long start;

				private long end;

				private readonly org.apache.hadoop.fs.FSDataInputStream underLyingStream;

				private readonly byte[] oneBytebuff = new byte[1];

				/// <exception cref="System.IO.IOException"/>
				internal HarFsInputStream(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
					 path, long start, long length, int bufferSize)
				{
					//The underlying data input stream that the
					// underlying filesystem will return.
					//one byte buffer
					if (length < 0)
					{
						throw new System.ArgumentException("Negative length [" + length + "]");
					}
					underLyingStream = fs.open(path, bufferSize);
					underLyingStream.seek(start);
					// the start of this file in the part file
					this.start = start;
					// the position pointer in the part file
					this.position = start;
					// the end pointer in the part file
					this.end = start + length;
				}

				/// <exception cref="System.IO.IOException"/>
				public override int available()
				{
					lock (this)
					{
						long remaining = end - underLyingStream.getPos();
						if (remaining > int.MaxValue)
						{
							return int.MaxValue;
						}
						return (int)remaining;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override void close()
				{
					lock (this)
					{
						underLyingStream.close();
						base.close();
					}
				}

				//not implemented
				public override void mark(int readLimit)
				{
				}

				// do nothing 
				/// <summary>reset is not implemented</summary>
				/// <exception cref="System.IO.IOException"/>
				public override void reset()
				{
					throw new System.IO.IOException("reset not implemented.");
				}

				/// <exception cref="System.IO.IOException"/>
				public override int read()
				{
					lock (this)
					{
						int ret = read(oneBytebuff, 0, 1);
						return (ret <= 0) ? -1 : (oneBytebuff[0] & unchecked((int)(0xff)));
					}
				}

				// NB: currently this method actually never executed becusae
				// java.io.DataInputStream.read(byte[]) directly delegates to 
				// method java.io.InputStream.read(byte[], int, int).
				// However, potentially it can be invoked, so leave it intact for now.
				/// <exception cref="System.IO.IOException"/>
				public override int read(byte[] b)
				{
					lock (this)
					{
						int ret = read(b, 0, b.Length);
						return ret;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override int read(byte[] b, int offset, int len)
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
						ret = underLyingStream.read(b, offset, newlen);
						position += ret;
						return ret;
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override long skip(long n)
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
							underLyingStream.seek(tmpN + position);
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
				public override long getPos()
				{
					lock (this)
					{
						return (position - start);
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override void seek(long pos)
				{
					lock (this)
					{
						validatePosition(pos);
						position = start + pos;
						underLyingStream.seek(position);
					}
				}

				/// <exception cref="System.IO.IOException"/>
				private void validatePosition(long pos)
				{
					if (pos < 0)
					{
						throw new System.IO.IOException("Negative position: " + pos);
					}
					long length = end - start;
					if (pos > length)
					{
						throw new System.IO.IOException("Position behind the end " + "of the stream (length = "
							 + length + "): " + pos);
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override bool seekToNewSource(long targetPos)
				{
					// do not need to implement this
					// hdfs in itself does seektonewsource
					// while reading.
					return false;
				}

				/// <summary>implementing position readable.</summary>
				/// <exception cref="System.IO.IOException"/>
				public override int read(long pos, byte[] b, int offset, int length)
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
					return underLyingStream.read(pos + start, b, offset, nlength);
				}

				/// <summary>position readable again.</summary>
				/// <exception cref="System.IO.IOException"/>
				public override void readFully(long pos, byte[] b, int offset, int length)
				{
					if (start + length + pos > end)
					{
						throw new System.IO.IOException("Not enough bytes to read.");
					}
					underLyingStream.readFully(pos + start, b, offset, length);
				}

				/// <exception cref="System.IO.IOException"/>
				public override void readFully(long pos, byte[] b)
				{
					readFully(pos, b, 0, b.Length);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void setReadahead(long readahead)
				{
					underLyingStream.setReadahead(readahead);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void setDropBehind(bool dropBehind)
				{
					underLyingStream.setDropBehind(dropBehind);
				}
			}

			/// <summary>constructors for har input stream.</summary>
			/// <param name="fs">the underlying filesystem</param>
			/// <param name="p">The path in the underlying filesystem</param>
			/// <param name="start">the start position in the part file</param>
			/// <param name="length">the length of valid data in the part file</param>
			/// <param name="bufsize">the buffer size</param>
			/// <exception cref="System.IO.IOException"/>
			public HarFSDataInputStream(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
				 p, long start, long length, int bufsize)
				: base(new org.apache.hadoop.fs.HarFileSystem.HarFSDataInputStream.HarFsInputStream
					(fs, p, start, length, bufsize))
			{
			}
		}

		private class HarMetaData
		{
			private org.apache.hadoop.fs.FileSystem fs;

			private int version;

			private org.apache.hadoop.fs.Path masterIndexPath;

			private org.apache.hadoop.fs.Path archiveIndexPath;

			private long masterIndexTimestamp;

			private long archiveIndexTimestamp;

			internal System.Collections.Generic.IList<org.apache.hadoop.fs.HarFileSystem.Store
				> stores = new System.Collections.Generic.List<org.apache.hadoop.fs.HarFileSystem.Store
				>();

			internal System.Collections.Generic.IDictionary<org.apache.hadoop.fs.Path, org.apache.hadoop.fs.HarFileSystem.HarStatus
				> archive = new System.Collections.Generic.Dictionary<org.apache.hadoop.fs.Path, 
				org.apache.hadoop.fs.HarFileSystem.HarStatus>();

			private System.Collections.Generic.IDictionary<org.apache.hadoop.fs.Path, org.apache.hadoop.fs.FileStatus
				> partFileStatuses = new System.Collections.Generic.Dictionary<org.apache.hadoop.fs.Path
				, org.apache.hadoop.fs.FileStatus>();

			public HarMetaData(HarFileSystem _enclosing, org.apache.hadoop.fs.FileSystem fs, 
				org.apache.hadoop.fs.Path masterIndexPath, org.apache.hadoop.fs.Path archiveIndexPath
				)
			{
				this._enclosing = _enclosing;
				// the masterIndex of the archive
				// the index file 
				this.fs = fs;
				this.masterIndexPath = masterIndexPath;
				this.archiveIndexPath = archiveIndexPath;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FileStatus getPartFileStatus(org.apache.hadoop.fs.Path
				 partPath)
			{
				org.apache.hadoop.fs.FileStatus status;
				status = this.partFileStatuses[partPath];
				if (status == null)
				{
					status = this.fs.getFileStatus(partPath);
					this.partFileStatuses[partPath] = status;
				}
				return status;
			}

			public virtual long getMasterIndexTimestamp()
			{
				return this.masterIndexTimestamp;
			}

			public virtual long getArchiveIndexTimestamp()
			{
				return this.archiveIndexTimestamp;
			}

			private int getVersion()
			{
				return this.version;
			}

			/// <exception cref="System.IO.IOException"/>
			private void parseMetaData()
			{
				org.apache.hadoop.io.Text line = new org.apache.hadoop.io.Text();
				long read;
				org.apache.hadoop.fs.FSDataInputStream @in = null;
				org.apache.hadoop.util.LineReader lin = null;
				try
				{
					@in = this.fs.open(this.masterIndexPath);
					org.apache.hadoop.fs.FileStatus masterStat = this.fs.getFileStatus(this.masterIndexPath
						);
					this.masterIndexTimestamp = masterStat.getModificationTime();
					lin = new org.apache.hadoop.util.LineReader(@in, this._enclosing.getConf());
					read = lin.readLine(line);
					// the first line contains the version of the index file
					string versionLine = line.ToString();
					string[] arr = versionLine.split(" ");
					this.version = System.Convert.ToInt32(arr[0]);
					// make it always backwards-compatible
					if (this.version > org.apache.hadoop.fs.HarFileSystem.VERSION)
					{
						throw new System.IO.IOException("Invalid version " + this.version + " expected " 
							+ org.apache.hadoop.fs.HarFileSystem.VERSION);
					}
					// each line contains a hashcode range and the index file name
					string[] readStr;
					while (read < masterStat.getLen())
					{
						int b = lin.readLine(line);
						read += b;
						readStr = line.ToString().split(" ");
						this.stores.add(new org.apache.hadoop.fs.HarFileSystem.Store(long.Parse(readStr[2
							]), long.Parse(readStr[3])));
						line.clear();
					}
				}
				catch (System.IO.IOException ioe)
				{
					org.apache.hadoop.fs.HarFileSystem.LOG.warn("Encountered exception ", ioe);
					throw;
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.cleanup(org.apache.hadoop.fs.HarFileSystem.LOG, lin, 
						@in);
				}
				org.apache.hadoop.fs.FSDataInputStream aIn = this.fs.open(this.archiveIndexPath);
				try
				{
					org.apache.hadoop.fs.FileStatus archiveStat = this.fs.getFileStatus(this.archiveIndexPath
						);
					this.archiveIndexTimestamp = archiveStat.getModificationTime();
					org.apache.hadoop.util.LineReader aLin;
					// now start reading the real index file
					foreach (org.apache.hadoop.fs.HarFileSystem.Store s in this.stores)
					{
						read = 0;
						aIn.seek(s.begin);
						aLin = new org.apache.hadoop.util.LineReader(aIn, this._enclosing.getConf());
						while (read + s.begin < s.end)
						{
							int tmp = aLin.readLine(line);
							read += tmp;
							string lineFeed = line.ToString();
							string[] parsed = lineFeed.split(" ");
							parsed[0] = this._enclosing.decodeFileName(parsed[0]);
							this.archive[new org.apache.hadoop.fs.Path(parsed[0])] = new org.apache.hadoop.fs.HarFileSystem.HarStatus
								(this, lineFeed);
							line.clear();
						}
					}
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.cleanup(org.apache.hadoop.fs.HarFileSystem.LOG, aIn);
				}
			}

			private readonly HarFileSystem _enclosing;
		}

		/*
		* testing purposes only:
		*/
		internal virtual org.apache.hadoop.fs.HarFileSystem.HarMetaData getMetadata()
		{
			return metadata;
		}

		[System.Serializable]
		private class LruCache<K, V> : java.util.LinkedHashMap<K, V>
		{
			private readonly int MAX_ENTRIES;

			public LruCache(int maxEntries)
				: base(maxEntries + 1, 1.0f, true)
			{
				MAX_ENTRIES = maxEntries;
			}

			protected override bool removeEldestEntry(System.Collections.Generic.KeyValuePair
				<K, V> eldest)
			{
				return Count > MAX_ENTRIES;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return fs.getServerDefaults();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getServerDefaults(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long getUsed()
		{
			return fs.getUsed();
		}

		public override long getDefaultBlockSize()
		{
			return fs.getDefaultBlockSize();
		}

		public override long getDefaultBlockSize(org.apache.hadoop.fs.Path f)
		{
			return fs.getDefaultBlockSize(f);
		}

		public override short getDefaultReplication()
		{
			return fs.getDefaultReplication();
		}

		public override short getDefaultReplication(org.apache.hadoop.fs.Path f)
		{
			return fs.getDefaultReplication(f);
		}
	}
}

using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// This class provides an interface for implementors of a Hadoop file system
	/// (analogous to the VFS of Unix).
	/// </summary>
	/// <remarks>
	/// This class provides an interface for implementors of a Hadoop file system
	/// (analogous to the VFS of Unix). Applications do not access this class;
	/// instead they access files across all file systems using
	/// <see cref="FileContext"/>
	/// .
	/// Pathnames passed to AbstractFileSystem can be fully qualified URI that
	/// matches the "this" file system (ie same scheme and authority)
	/// or a Slash-relative name that is assumed to be relative
	/// to the root of the "this" file system .
	/// </remarks>
	public abstract class AbstractFileSystem
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.AbstractFileSystem
			)));

		/// <summary>Recording statistics per a file system class.</summary>
		private static readonly System.Collections.Generic.IDictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
			> STATISTICS_TABLE = new System.Collections.Generic.Dictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
			>();

		/// <summary>Cache of constructors for each file system class.</summary>
		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, java.lang.reflect.Constructor
			<object>> CONSTRUCTOR_CACHE = new java.util.concurrent.ConcurrentHashMap<java.lang.Class
			, java.lang.reflect.Constructor<object>>();

		private static readonly java.lang.Class[] URI_CONFIG_ARGS = new java.lang.Class[]
			 { Sharpen.Runtime.getClassForType(typeof(java.net.URI)), Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.conf.Configuration)) };

		/// <summary>The statistics for this file system.</summary>
		protected internal org.apache.hadoop.fs.FileSystem.Statistics statistics;

		[com.google.common.annotations.VisibleForTesting]
		internal const string NO_ABSTRACT_FS_ERROR = "No AbstractFileSystem configured for scheme";

		private readonly java.net.URI myUri;

		/*Evolving for a release,to be changed to Stable */
		public virtual org.apache.hadoop.fs.FileSystem.Statistics getStatistics()
		{
			return statistics;
		}

		/// <summary>
		/// Returns true if the specified string is considered valid in the path part
		/// of a URI by this file system.
		/// </summary>
		/// <remarks>
		/// Returns true if the specified string is considered valid in the path part
		/// of a URI by this file system.  The default implementation enforces the rules
		/// of HDFS, but subclasses may override this method to implement specific
		/// validation rules for specific file systems.
		/// </remarks>
		/// <param name="src">String source filename to check, path part of the URI</param>
		/// <returns>boolean true if the specified string is considered valid</returns>
		public virtual bool isValidName(string src)
		{
			// Prohibit ".." "." and anything containing ":"
			java.util.StringTokenizer tokens = new java.util.StringTokenizer(src, org.apache.hadoop.fs.Path
				.SEPARATOR);
			while (tokens.hasMoreTokens())
			{
				string element = tokens.nextToken();
				if (element.Equals("..") || element.Equals(".") || (element.IndexOf(":") >= 0))
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>Create an object for the given class and initialize it from conf.</summary>
		/// <param name="theClass">class of which an object is created</param>
		/// <param name="conf">Configuration</param>
		/// <returns>a new object</returns>
		internal static T newInstance<T>(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			System.Type theClass = typeof(T);
			T result;
			try
			{
				java.lang.reflect.Constructor<T> meth = (java.lang.reflect.Constructor<T>)CONSTRUCTOR_CACHE
					[theClass];
				if (meth == null)
				{
					meth = theClass.getDeclaredConstructor(URI_CONFIG_ARGS);
					meth.setAccessible(true);
					CONSTRUCTOR_CACHE[theClass] = meth;
				}
				result = meth.newInstance(uri, conf);
			}
			catch (System.Exception e)
			{
				throw new System.Exception(e);
			}
			return result;
		}

		/// <summary>Create a file system instance for the specified uri using the conf.</summary>
		/// <remarks>
		/// Create a file system instance for the specified uri using the conf. The
		/// conf is used to find the class name that implements the file system. The
		/// conf is also passed to the file system for its configuration.
		/// </remarks>
		/// <param name="uri">URI of the file system</param>
		/// <param name="conf">Configuration for the file system</param>
		/// <returns>Returns the file system for the given URI</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// file system for <code>uri</code> is
		/// not found
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.AbstractFileSystem createFileSystem(java.net.URI
			 uri, org.apache.hadoop.conf.Configuration conf)
		{
			string fsImplConf = string.format("fs.AbstractFileSystem.%s.impl", uri.getScheme(
				));
			java.lang.Class clazz = conf.getClass(fsImplConf, null);
			if (clazz == null)
			{
				throw new org.apache.hadoop.fs.UnsupportedFileSystemException(string.format("%s=null: %s: %s"
					, fsImplConf, NO_ABSTRACT_FS_ERROR, uri.getScheme()));
			}
			return (org.apache.hadoop.fs.AbstractFileSystem)newInstance(clazz, uri, conf);
		}

		/// <summary>Get the statistics for a particular file system.</summary>
		/// <param name="uri">
		/// used as key to lookup STATISTICS_TABLE. Only scheme and authority
		/// part of the uri are used.
		/// </param>
		/// <returns>a statistics object</returns>
		protected internal static org.apache.hadoop.fs.FileSystem.Statistics getStatistics
			(java.net.URI uri)
		{
			lock (typeof(AbstractFileSystem))
			{
				string scheme = uri.getScheme();
				if (scheme == null)
				{
					throw new System.ArgumentException("Scheme not defined in the uri: " + uri);
				}
				java.net.URI baseUri = getBaseUri(uri);
				org.apache.hadoop.fs.FileSystem.Statistics result = STATISTICS_TABLE[baseUri];
				if (result == null)
				{
					result = new org.apache.hadoop.fs.FileSystem.Statistics(scheme);
					STATISTICS_TABLE[baseUri] = result;
				}
				return result;
			}
		}

		private static java.net.URI getBaseUri(java.net.URI uri)
		{
			string scheme = uri.getScheme();
			string authority = uri.getAuthority();
			string baseUriString = scheme + "://";
			if (authority != null)
			{
				baseUriString = baseUriString + authority;
			}
			else
			{
				baseUriString = baseUriString + "/";
			}
			return java.net.URI.create(baseUriString);
		}

		public static void clearStatistics()
		{
			lock (typeof(AbstractFileSystem))
			{
				foreach (org.apache.hadoop.fs.FileSystem.Statistics stat in STATISTICS_TABLE.Values)
				{
					stat.reset();
				}
			}
		}

		/// <summary>Prints statistics for all file systems.</summary>
		public static void printStatistics()
		{
			lock (typeof(AbstractFileSystem))
			{
				foreach (System.Collections.Generic.KeyValuePair<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
					> pair in STATISTICS_TABLE)
				{
					System.Console.Out.WriteLine("  FileSystem " + pair.Key.getScheme() + "://" + pair
						.Key.getAuthority() + ": " + pair.Value);
				}
			}
		}

		protected internal static System.Collections.Generic.IDictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
			> getAllStatistics()
		{
			lock (typeof(AbstractFileSystem))
			{
				System.Collections.Generic.IDictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
					> statsMap = new System.Collections.Generic.Dictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
					>(STATISTICS_TABLE.Count);
				foreach (System.Collections.Generic.KeyValuePair<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
					> pair in STATISTICS_TABLE)
				{
					java.net.URI key = pair.Key;
					org.apache.hadoop.fs.FileSystem.Statistics value = pair.Value;
					org.apache.hadoop.fs.FileSystem.Statistics newStatsObj = new org.apache.hadoop.fs.FileSystem.Statistics
						(value);
					statsMap[java.net.URI.create(key.ToString())] = newStatsObj;
				}
				return statsMap;
			}
		}

		/// <summary>The main factory method for creating a file system.</summary>
		/// <remarks>
		/// The main factory method for creating a file system. Get a file system for
		/// the URI's scheme and authority. The scheme of the <code>uri</code>
		/// determines a configuration property name,
		/// <tt>fs.AbstractFileSystem.<i>scheme</i>.impl</tt> whose value names the
		/// AbstractFileSystem class.
		/// The entire URI and conf is passed to the AbstractFileSystem factory method.
		/// </remarks>
		/// <param name="uri">for the file system to be created.</param>
		/// <param name="conf">which is passed to the file system impl.</param>
		/// <returns>file system for the given URI.</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// if the file system for
		/// <code>uri</code> is not supported.
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.AbstractFileSystem get(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			return createFileSystem(uri, conf);
		}

		/// <summary>Constructor to be called by subclasses.</summary>
		/// <param name="uri">for this file system.</param>
		/// <param name="supportedScheme">the scheme supported by the implementor</param>
		/// <param name="authorityNeeded">
		/// if true then theURI must have authority, if false
		/// then the URI must have null authority.
		/// </param>
		/// <exception cref="java.net.URISyntaxException"><code>uri</code> has syntax error</exception>
		public AbstractFileSystem(java.net.URI uri, string supportedScheme, bool authorityNeeded
			, int defaultPort)
		{
			myUri = getUri(uri, supportedScheme, authorityNeeded, defaultPort);
			statistics = getStatistics(uri);
		}

		/// <summary>Check that the Uri's scheme matches</summary>
		/// <param name="uri"/>
		/// <param name="supportedScheme"/>
		public virtual void checkScheme(java.net.URI uri, string supportedScheme)
		{
			string scheme = uri.getScheme();
			if (scheme == null)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Uri without scheme: "
					 + uri);
			}
			if (!scheme.Equals(supportedScheme))
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Uri scheme " + uri + 
					" does not match the scheme " + supportedScheme);
			}
		}

		/// <summary>Get the URI for the file system based on the given URI.</summary>
		/// <remarks>
		/// Get the URI for the file system based on the given URI. The path, query
		/// part of the given URI is stripped out and default file system port is used
		/// to form the URI.
		/// </remarks>
		/// <param name="uri">FileSystem URI.</param>
		/// <param name="authorityNeeded">
		/// if true authority cannot be null in the URI. If
		/// false authority must be null.
		/// </param>
		/// <param name="defaultPort">default port to use if port is not specified in the URI.
		/// 	</param>
		/// <returns>URI of the file system</returns>
		/// <exception cref="java.net.URISyntaxException"><code>uri</code> has syntax error</exception>
		private java.net.URI getUri(java.net.URI uri, string supportedScheme, bool authorityNeeded
			, int defaultPort)
		{
			checkScheme(uri, supportedScheme);
			// A file system implementation that requires authority must always
			// specify default port
			if (defaultPort < 0 && authorityNeeded)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("FileSystem implementation error -  default port "
					 + defaultPort + " is not valid");
			}
			string authority = uri.getAuthority();
			if (authority == null)
			{
				if (authorityNeeded)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Uri without authority: "
						 + uri);
				}
				else
				{
					return new java.net.URI(supportedScheme + ":///");
				}
			}
			// authority is non null  - AuthorityNeeded may be true or false.
			int port = uri.getPort();
			port = (port == -1 ? defaultPort : port);
			if (port == -1)
			{
				// no port supplied and default port is not specified
				return new java.net.URI(supportedScheme, authority, "/", null);
			}
			return new java.net.URI(supportedScheme + "://" + uri.getHost() + ":" + port);
		}

		/// <summary>The default port of this file system.</summary>
		/// <returns>
		/// default port of this file system's Uri scheme
		/// A uri with a port of -1 =&gt; default port;
		/// </returns>
		public abstract int getUriDefaultPort();

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		/// <returns>the uri of this file system.</returns>
		public virtual java.net.URI getUri()
		{
			return myUri;
		}

		/// <summary>Check that a Path belongs to this FileSystem.</summary>
		/// <remarks>
		/// Check that a Path belongs to this FileSystem.
		/// If the path is fully qualified URI, then its scheme and authority
		/// matches that of this file system. Otherwise the path must be
		/// slash-relative name.
		/// </remarks>
		/// <exception cref="InvalidPathException">if the path is invalid</exception>
		public virtual void checkPath(org.apache.hadoop.fs.Path path)
		{
			java.net.URI uri = path.toUri();
			string thatScheme = uri.getScheme();
			string thatAuthority = uri.getAuthority();
			if (thatScheme == null)
			{
				if (thatAuthority == null)
				{
					if (path.isUriPathAbsolute())
					{
						return;
					}
					throw new org.apache.hadoop.fs.InvalidPathException("relative paths not allowed:"
						 + path);
				}
				else
				{
					throw new org.apache.hadoop.fs.InvalidPathException("Path without scheme with non-null authority:"
						 + path);
				}
			}
			string thisScheme = this.getUri().getScheme();
			string thisHost = this.getUri().getHost();
			string thatHost = uri.getHost();
			// Schemes and hosts must match.
			// Allow for null Authority for file:///
			if (!Sharpen.Runtime.equalsIgnoreCase(thisScheme, thatScheme) || (thisHost != null
				 && !Sharpen.Runtime.equalsIgnoreCase(thisHost, thatHost)) || (thisHost == null 
				&& thatHost != null))
			{
				throw new org.apache.hadoop.fs.InvalidPathException("Wrong FS: " + path + ", expected: "
					 + this.getUri());
			}
			// Ports must match, unless this FS instance is using the default port, in
			// which case the port may be omitted from the given URI
			int thisPort = this.getUri().getPort();
			int thatPort = uri.getPort();
			if (thatPort == -1)
			{
				// -1 => defaultPort of Uri scheme
				thatPort = this.getUriDefaultPort();
			}
			if (thisPort != thatPort)
			{
				throw new org.apache.hadoop.fs.InvalidPathException("Wrong FS: " + path + ", expected: "
					 + this.getUri());
			}
		}

		/// <summary>Get the path-part of a pathname.</summary>
		/// <remarks>
		/// Get the path-part of a pathname. Checks that URI matches this file system
		/// and that the path-part is a valid name.
		/// </remarks>
		/// <param name="p">path</param>
		/// <returns>path-part of the Path p</returns>
		public virtual string getUriPath(org.apache.hadoop.fs.Path p)
		{
			checkPath(p);
			string s = p.toUri().getPath();
			if (!isValidName(s))
			{
				throw new org.apache.hadoop.fs.InvalidPathException("Path part " + s + " from URI "
					 + p + " is not a valid filename.");
			}
			return s;
		}

		/// <summary>Make the path fully qualified to this file system</summary>
		/// <param name="path"/>
		/// <returns>the qualified path</returns>
		public virtual org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path 
			path)
		{
			checkPath(path);
			return path.makeQualified(this.getUri(), null);
		}

		/// <summary>
		/// Some file systems like LocalFileSystem have an initial workingDir
		/// that is used as the starting workingDir.
		/// </summary>
		/// <remarks>
		/// Some file systems like LocalFileSystem have an initial workingDir
		/// that is used as the starting workingDir. For other file systems
		/// like HDFS there is no built in notion of an initial workingDir.
		/// </remarks>
		/// <returns>
		/// the initial workingDir if the file system has such a notion
		/// otherwise return a null.
		/// </returns>
		public virtual org.apache.hadoop.fs.Path getInitialWorkingDirectory()
		{
			return null;
		}

		/// <summary>Return the current user's home directory in this file system.</summary>
		/// <remarks>
		/// Return the current user's home directory in this file system.
		/// The default implementation returns "/user/$USER/".
		/// </remarks>
		/// <returns>current user's home directory.</returns>
		public virtual org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return new org.apache.hadoop.fs.Path("/user/" + Sharpen.Runtime.getProperty("user.name"
				)).makeQualified(getUri(), null);
		}

		/// <summary>Return a set of server default configuration values.</summary>
		/// <returns>server default configuration values</returns>
		/// <exception cref="System.IO.IOException">an I/O error occurred</exception>
		public abstract org.apache.hadoop.fs.FsServerDefaults getServerDefaults();

		/// <summary>
		/// Return the fully-qualified path of path f resolving the path
		/// through any internal symlinks or mount point
		/// </summary>
		/// <param name="p">path to be resolved</param>
		/// <returns>fully qualified path</returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// , AccessControlException, IOException
		/// UnresolvedLinkException if symbolic link on path cannot be resolved
		/// internally
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p)
		{
			checkPath(p);
			return getFileStatus(p).getPath();
		}

		// default impl is to return the path
		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.create(Path, java.util.EnumSet{E}, CreateOpts[])"/>
		/// except
		/// that the Path f must be fully qualified and the permission is absolute
		/// (i.e. umask has been applied).
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f
			, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, params org.apache.hadoop.fs.Options.CreateOpts
			[] opts)
		{
			checkPath(f);
			int bufferSize = -1;
			short replication = -1;
			long blockSize = -1;
			int bytesPerChecksum = -1;
			org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt = null;
			org.apache.hadoop.fs.permission.FsPermission permission = null;
			org.apache.hadoop.util.Progressable progress = null;
			bool createParent = null;
			foreach (org.apache.hadoop.fs.Options.CreateOpts iOpt in opts)
			{
				if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.BlockSize
					)).isInstance(iOpt))
				{
					if (blockSize != -1)
					{
						throw new org.apache.hadoop.HadoopIllegalArgumentException("BlockSize option is set multiple times"
							);
					}
					blockSize = ((org.apache.hadoop.fs.Options.CreateOpts.BlockSize)iOpt).getValue();
				}
				else
				{
					if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.BufferSize
						)).isInstance(iOpt))
					{
						if (bufferSize != -1)
						{
							throw new org.apache.hadoop.HadoopIllegalArgumentException("BufferSize option is set multiple times"
								);
						}
						bufferSize = ((org.apache.hadoop.fs.Options.CreateOpts.BufferSize)iOpt).getValue(
							);
					}
					else
					{
						if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.ReplicationFactor
							)).isInstance(iOpt))
						{
							if (replication != -1)
							{
								throw new org.apache.hadoop.HadoopIllegalArgumentException("ReplicationFactor option is set multiple times"
									);
							}
							replication = ((org.apache.hadoop.fs.Options.CreateOpts.ReplicationFactor)iOpt).getValue
								();
						}
						else
						{
							if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.BytesPerChecksum
								)).isInstance(iOpt))
							{
								if (bytesPerChecksum != -1)
								{
									throw new org.apache.hadoop.HadoopIllegalArgumentException("BytesPerChecksum option is set multiple times"
										);
								}
								bytesPerChecksum = ((org.apache.hadoop.fs.Options.CreateOpts.BytesPerChecksum)iOpt
									).getValue();
							}
							else
							{
								if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.ChecksumParam
									)).isInstance(iOpt))
								{
									if (checksumOpt != null)
									{
										throw new org.apache.hadoop.HadoopIllegalArgumentException("CreateChecksumType option is set multiple times"
											);
									}
									checksumOpt = ((org.apache.hadoop.fs.Options.CreateOpts.ChecksumParam)iOpt).getValue
										();
								}
								else
								{
									if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.Perms
										)).isInstance(iOpt))
									{
										if (permission != null)
										{
											throw new org.apache.hadoop.HadoopIllegalArgumentException("Perms option is set multiple times"
												);
										}
										permission = ((org.apache.hadoop.fs.Options.CreateOpts.Perms)iOpt).getValue();
									}
									else
									{
										if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.Progress
											)).isInstance(iOpt))
										{
											if (progress != null)
											{
												throw new org.apache.hadoop.HadoopIllegalArgumentException("Progress option is set multiple times"
													);
											}
											progress = ((org.apache.hadoop.fs.Options.CreateOpts.Progress)iOpt).getValue();
										}
										else
										{
											if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Options.CreateOpts.CreateParent
												)).isInstance(iOpt))
											{
												if (createParent != null)
												{
													throw new org.apache.hadoop.HadoopIllegalArgumentException("CreateParent option is set multiple times"
														);
												}
												createParent = ((org.apache.hadoop.fs.Options.CreateOpts.CreateParent)iOpt).getValue
													();
											}
											else
											{
												throw new org.apache.hadoop.HadoopIllegalArgumentException("Unkown CreateOpts of type "
													 + Sharpen.Runtime.getClassForObject(iOpt).getName());
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (permission == null)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("no permission supplied"
					);
			}
			org.apache.hadoop.fs.FsServerDefaults ssDef = getServerDefaults();
			if (ssDef.getBlockSize() % ssDef.getBytesPerChecksum() != 0)
			{
				throw new System.IO.IOException("Internal error: default blockSize is" + " not a multiple of default bytesPerChecksum "
					);
			}
			if (blockSize == -1)
			{
				blockSize = ssDef.getBlockSize();
			}
			// Create a checksum option honoring user input as much as possible.
			// If bytesPerChecksum is specified, it will override the one set in
			// checksumOpt. Any missing value will be filled in using the default.
			org.apache.hadoop.fs.Options.ChecksumOpt defaultOpt = new org.apache.hadoop.fs.Options.ChecksumOpt
				(ssDef.getChecksumType(), ssDef.getBytesPerChecksum());
			checksumOpt = org.apache.hadoop.fs.Options.ChecksumOpt.processChecksumOpt(defaultOpt
				, checksumOpt, bytesPerChecksum);
			if (bufferSize == -1)
			{
				bufferSize = ssDef.getFileBufferSize();
			}
			if (replication == -1)
			{
				replication = ssDef.getReplication();
			}
			if (createParent == null)
			{
				createParent = false;
			}
			if (blockSize % bytesPerChecksum != 0)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("blockSize should be a multiple of checksumsize"
					);
			}
			return this.createInternal(f, createFlag, permission, bufferSize, replication, blockSize
				, progress, checksumOpt, createParent);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="create(Path, java.util.EnumSet{E}, CreateOpts[])"/>
		/// except that the opts
		/// have been declared explicitly.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
			);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.mkdir(Path, org.apache.hadoop.fs.permission.FsPermission, bool)
		/// 	"/>
		/// except that the Path
		/// f must be fully qualified and the permission is absolute (i.e.
		/// umask has been applied).
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.delete(Path, bool)"/>
		/// except that Path f must be for
		/// this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool delete(org.apache.hadoop.fs.Path f, bool recursive);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.open(Path)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f)
		{
			return open(f, getServerDefaults().getFileBufferSize());
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.open(Path, int)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.truncate(Path, long)"/>
		/// except that Path f must be for
		/// this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support truncate");
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.setReplication(Path, short)"/>
		/// except that Path f must be
		/// for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool setReplication(org.apache.hadoop.fs.Path f, short replication
			);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.rename(Path, Path, Rename[])"/>
		/// except that Path
		/// f must be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst, 
			params org.apache.hadoop.fs.Options.Rename[] options)
		{
			bool overwrite = false;
			if (null != options)
			{
				foreach (org.apache.hadoop.fs.Options.Rename option in options)
				{
					if (option == org.apache.hadoop.fs.Options.Rename.OVERWRITE)
					{
						overwrite = true;
					}
				}
			}
			renameInternal(src, dst, overwrite);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.rename(Path, Path, Rename[])"/>
		/// except that Path
		/// f must be for this file system and NO OVERWRITE is performed.
		/// File systems that do not have a built in overwrite need implement only this
		/// method and can take advantage of the default impl of the other
		/// <see cref="renameInternal(Path, Path, bool)"/>
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.rename(Path, Path, Rename[])"/>
		/// except that Path
		/// f must be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool overwrite)
		{
			// Default implementation deals with overwrite in a non-atomic way
			org.apache.hadoop.fs.FileStatus srcStatus = getFileLinkStatus(src);
			org.apache.hadoop.fs.FileStatus dstStatus;
			try
			{
				dstStatus = getFileLinkStatus(dst);
			}
			catch (System.IO.IOException)
			{
				dstStatus = null;
			}
			if (dstStatus != null)
			{
				if (dst.Equals(src))
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("The source " + src + " and destination "
						 + dst + " are the same");
				}
				if (srcStatus.isSymlink() && dst.Equals(srcStatus.getSymlink()))
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("Cannot rename symlink "
						 + src + " to its target " + dst);
				}
				// It's OK to rename a file to a symlink and vice versa
				if (srcStatus.isDirectory() != dstStatus.isDirectory())
				{
					throw new System.IO.IOException("Source " + src + " and destination " + dst + " must both be directories"
						);
				}
				if (!overwrite)
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("Rename destination " +
						 dst + " already exists.");
				}
				// Delete the destination that is a file or an empty directory
				if (dstStatus.isDirectory())
				{
					org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> list = listStatusIterator
						(dst);
					if (list != null && list.hasNext())
					{
						throw new System.IO.IOException("Rename cannot overwrite non empty destination directory "
							 + dst);
					}
				}
				delete(dst, false);
			}
			else
			{
				org.apache.hadoop.fs.Path parent = dst.getParent();
				org.apache.hadoop.fs.FileStatus parentStatus = getFileStatus(parent);
				if (parentStatus.isFile())
				{
					throw new org.apache.hadoop.fs.ParentNotDirectoryException("Rename destination parent "
						 + parent + " is a file.");
				}
			}
			renameInternal(src, dst);
		}

		/// <summary>Returns true if the file system supports symlinks, false otherwise.</summary>
		/// <returns>true if filesystem supports symlinks</returns>
		public virtual bool supportsSymlinks()
		{
			return false;
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.createSymlink(Path, Path, bool)"/>
		/// ;
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public virtual void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			throw new System.IO.IOException("File system does not support symlinks");
		}

		/// <summary>Partially resolves the path.</summary>
		/// <remarks>
		/// Partially resolves the path. This is used during symlink resolution in
		/// <see cref="FSLinkResolver{T}"/>
		/// , and differs from the similarly named method
		/// <see cref="FileContext.getLinkTarget(Path)"/>
		/// .
		/// </remarks>
		/// <exception cref="System.IO.IOException">subclass implementations may throw IOException
		/// 	</exception>
		public virtual org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path 
			f)
		{
			throw new java.lang.AssertionError("Implementation Error: " + Sharpen.Runtime.getClassForObject
				(this) + " that threw an UnresolvedLinkException, causing this method to be" + " called, needs to override this method."
				);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)
		/// 	"/>
		/// except that Path f
		/// must be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.setOwner(Path, string, string)"/>
		/// except that Path f must
		/// be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.setTimes(Path, long, long)"/>
		/// except that Path f must be
		/// for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.getFileChecksum(Path)"/>
		/// except that Path f must be for
		/// this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.getFileStatus(Path)"/>
		/// 
		/// except that an UnresolvedLinkException may be thrown if a symlink is
		/// encountered in the path.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.access(Path, org.apache.hadoop.fs.permission.FsAction)"/>
		/// except that an UnresolvedLinkException may be thrown if a symlink is
		/// encountered in the path.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			org.apache.hadoop.fs.FileSystem.checkAccessPermissions(this.getFileStatus(path), 
				mode);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.getFileLinkStatus(Path)"/>
		/// except that an UnresolvedLinkException may be thrown if a symlink is
		/// encountered in the path leading up to the final path component.
		/// If the file system does not support symlinks then the behavior is
		/// equivalent to
		/// <see cref="getFileStatus(Path)"/>
		/// .
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return getFileStatus(f);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.getFileBlockLocations(Path, long, long)"/>
		/// except that
		/// Path f must be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.getFsStatus(Path)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FsStatus getFsStatus(org.apache.hadoop.fs.Path
			 f)
		{
			// default impl gets FsStatus of root
			return getFsStatus();
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.getFsStatus(Path)"/>
		/// .
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FsStatus getFsStatus();

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.listStatus(Path)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path f)
		{
			return new _RemoteIterator_887(this, f);
		}

		private sealed class _RemoteIterator_887 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			>
		{
			public _RemoteIterator_887(org.apache.hadoop.fs.Path f)
			{
				this.f = f;
				this.i = 0;
				this.statusList = this._enclosing.listStatus(f);
			}

			private int i;

			private org.apache.hadoop.fs.FileStatus[] statusList;

			public bool hasNext()
			{
				return this.i < this.statusList.Length;
			}

			public org.apache.hadoop.fs.FileStatus next()
			{
				if (!this.hasNext())
				{
					throw new java.util.NoSuchElementException();
				}
				return this.statusList[this.i++];
			}

			private readonly org.apache.hadoop.fs.Path f;
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.listLocatedStatus(Path)"/>
		/// except that Path f
		/// must be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f)
		{
			return new _RemoteIterator_914(this, f);
		}

		private sealed class _RemoteIterator_914 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			>
		{
			public _RemoteIterator_914(AbstractFileSystem _enclosing, org.apache.hadoop.fs.Path
				 f)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.itor = this._enclosing.listStatusIterator(f);
			}

			private org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> itor;

			/// <exception cref="System.IO.IOException"/>
			public bool hasNext()
			{
				return this.itor.hasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.LocatedFileStatus next()
			{
				if (!this.hasNext())
				{
					throw new java.util.NoSuchElementException("No more entry in " + f);
				}
				org.apache.hadoop.fs.FileStatus result = this.itor.next();
				org.apache.hadoop.fs.BlockLocation[] locs = null;
				if (result.isFile())
				{
					locs = this._enclosing.getFileBlockLocations(result.getPath(), 0, result.getLen()
						);
				}
				return new org.apache.hadoop.fs.LocatedFileStatus(result, locs);
			}

			private readonly AbstractFileSystem _enclosing;

			private readonly org.apache.hadoop.fs.Path f;
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="Util.listStatus(Path)"/>
		/// except that Path f must be
		/// for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f);

		/// <returns>
		/// an iterator over the corrupt files under the given path
		/// (may contain duplicates if a file has more than one corrupt block)
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.Path> listCorruptFileBlocks
			(org.apache.hadoop.fs.Path path)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getCanonicalName
				() + " does not support" + " listCorruptFileBlocks");
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.setVerifyChecksum(bool, Path)"/>
		/// except that Path f
		/// must be for this file system.
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void setVerifyChecksum(bool verifyChecksum);

		/// <summary>Get a canonical name for this file system.</summary>
		/// <returns>a URI string that uniquely identifies this file system</returns>
		public virtual string getCanonicalServiceName()
		{
			return org.apache.hadoop.security.SecurityUtil.buildDTServiceName(getUri(), getUriDefaultPort
				());
		}

		/// <summary>Get one or more delegation tokens associated with the filesystem.</summary>
		/// <remarks>
		/// Get one or more delegation tokens associated with the filesystem. Normally
		/// a file system returns a single delegation token. A file system that manages
		/// multiple file systems underneath, could return set of delegation tokens for
		/// all the file systems it manages
		/// </remarks>
		/// <param name="renewer">the account name that is allowed to renew the token.</param>
		/// <returns>
		/// List of delegation tokens.
		/// If delegation tokens not supported then return a list of size zero.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> getDelegationTokens(string renewer)
		{
			return new System.Collections.Generic.List<org.apache.hadoop.security.token.Token
				<object>>(0);
		}

		/// <summary>Modifies ACL entries of files and directories.</summary>
		/// <remarks>
		/// Modifies ACL entries of files and directories.  This method can add new ACL
		/// entries or modify the permissions on existing ACL entries.  All existing
		/// ACL entries that are not specified in this call are retained without
		/// changes.  (Modifications are merged into the current ACL.)
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">List<AclEntry> describing modifications</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support modifyAclEntries");
		}

		/// <summary>Removes ACL entries from files and directories.</summary>
		/// <remarks>
		/// Removes ACL entries from files and directories.  Other ACL entries are
		/// retained.
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">List<AclEntry> describing entries to remove</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support removeAclEntries");
		}

		/// <summary>Removes all default ACL entries from files and directories.</summary>
		/// <param name="path">Path to modify</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support removeDefaultAcl");
		}

		/// <summary>Removes all but the base ACL entries of files and directories.</summary>
		/// <remarks>
		/// Removes all but the base ACL entries of files and directories.  The entries
		/// for user, group, and others are retained for compatibility with permission
		/// bits.
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be removed</exception>
		public virtual void removeAcl(org.apache.hadoop.fs.Path path)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support removeAcl");
		}

		/// <summary>
		/// Fully replaces ACL of files and directories, discarding all existing
		/// entries.
		/// </summary>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">
		/// List<AclEntry> describing modifications, must include entries
		/// for user, group, and others for compatibility with permission bits.
		/// </param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support setAcl");
		}

		/// <summary>Gets the ACLs of files and directories.</summary>
		/// <param name="path">Path to get</param>
		/// <returns>RemoteIterator<AclStatus> which returns each AclStatus</returns>
		/// <exception cref="System.IO.IOException">if an ACL could not be read</exception>
		public virtual org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support getAclStatus");
		}

		/// <summary>Set an xattr of a file or directory.</summary>
		/// <remarks>
		/// Set an xattr of a file or directory.
		/// The name must be prefixed with the namespace followed by ".". For example,
		/// "user.attr".
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <param name="name">xattr name.</param>
		/// <param name="value">xattr value.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[] 
			value)
		{
			setXAttr(path, name, value, java.util.EnumSet.of(org.apache.hadoop.fs.XAttrSetFlag
				.CREATE, org.apache.hadoop.fs.XAttrSetFlag.REPLACE));
		}

		/// <summary>Set an xattr of a file or directory.</summary>
		/// <remarks>
		/// Set an xattr of a file or directory.
		/// The name must be prefixed with the namespace followed by ".". For example,
		/// "user.attr".
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <param name="name">xattr name.</param>
		/// <param name="value">xattr value.</param>
		/// <param name="flag">xattr set flag</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[] 
			value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support setXAttr");
		}

		/// <summary>Get an xattr for a file or directory.</summary>
		/// <remarks>
		/// Get an xattr for a file or directory.
		/// The name must be prefixed with the namespace followed by ".". For example,
		/// "user.attr".
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to get extended attribute</param>
		/// <param name="name">xattr name.</param>
		/// <returns>byte[] xattr value.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support getXAttr");
		}

		/// <summary>Get all of the xattrs for a file or directory.</summary>
		/// <remarks>
		/// Get all of the xattrs for a file or directory.
		/// Only those xattrs for which the logged-in user has permissions to view
		/// are returned.
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to get extended attributes</param>
		/// <returns>Map<String, byte[]> describing the XAttrs of the file or directory</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support getXAttrs");
		}

		/// <summary>Get all of the xattrs for a file or directory.</summary>
		/// <remarks>
		/// Get all of the xattrs for a file or directory.
		/// Only those xattrs for which the logged-in user has permissions to view
		/// are returned.
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to get extended attributes</param>
		/// <param name="names">XAttr names.</param>
		/// <returns>Map<String, byte[]> describing the XAttrs of the file or directory</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(org.apache.hadoop.fs.Path
			 path, System.Collections.Generic.IList<string> names)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support getXAttrs");
		}

		/// <summary>Get all of the xattr names for a file or directory.</summary>
		/// <remarks>
		/// Get all of the xattr names for a file or directory.
		/// Only the xattr names for which the logged-in user has permissions to view
		/// are returned.
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to get extended attributes</param>
		/// <returns>Map<String, byte[]> describing the XAttrs of the file or directory</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support listXAttrs");
		}

		/// <summary>Remove an xattr of a file or directory.</summary>
		/// <remarks>
		/// Remove an xattr of a file or directory.
		/// The name must be prefixed with the namespace followed by ".". For example,
		/// "user.attr".
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to remove extended attribute</param>
		/// <param name="name">xattr name</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support removeXAttr");
		}

		public override int GetHashCode()
		{
			//Object
			return myUri.GetHashCode();
		}

		public override bool Equals(object other)
		{
			//Object
			if (other == null || !(other is org.apache.hadoop.fs.AbstractFileSystem))
			{
				return false;
			}
			return myUri.Equals(((org.apache.hadoop.fs.AbstractFileSystem)other).myUri);
		}
	}
}

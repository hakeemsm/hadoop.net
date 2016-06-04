using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.FS
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
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.AbstractFileSystem
			));

		/// <summary>Recording statistics per a file system class.</summary>
		private static readonly IDictionary<URI, FileSystem.Statistics> StatisticsTable = 
			new Dictionary<URI, FileSystem.Statistics>();

		/// <summary>Cache of constructors for each file system class.</summary>
		private static readonly IDictionary<Type, Constructor<object>> ConstructorCache = 
			new ConcurrentHashMap<Type, Constructor<object>>();

		private static readonly Type[] UriConfigArgs = new Type[] { typeof(URI), typeof(Configuration
			) };

		/// <summary>The statistics for this file system.</summary>
		protected internal FileSystem.Statistics statistics;

		[VisibleForTesting]
		internal const string NoAbstractFsError = "No AbstractFileSystem configured for scheme";

		private readonly URI myUri;

		/*Evolving for a release,to be changed to Stable */
		public virtual FileSystem.Statistics GetStatistics()
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
		public virtual bool IsValidName(string src)
		{
			// Prohibit ".." "." and anything containing ":"
			StringTokenizer tokens = new StringTokenizer(src, Path.Separator);
			while (tokens.HasMoreTokens())
			{
				string element = tokens.NextToken();
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
		internal static T NewInstance<T>(URI uri, Configuration conf)
		{
			System.Type theClass = typeof(T);
			T result;
			try
			{
				Constructor<T> meth = (Constructor<T>)ConstructorCache[theClass];
				if (meth == null)
				{
					meth = theClass.GetDeclaredConstructor(UriConfigArgs);
					ConstructorCache[theClass] = meth;
				}
				result = meth.NewInstance(uri, conf);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
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
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public static Org.Apache.Hadoop.FS.AbstractFileSystem CreateFileSystem(URI uri, Configuration
			 conf)
		{
			string fsImplConf = string.Format("fs.AbstractFileSystem.%s.impl", uri.GetScheme(
				));
			Type clazz = conf.GetClass(fsImplConf, null);
			if (clazz == null)
			{
				throw new UnsupportedFileSystemException(string.Format("%s=null: %s: %s", fsImplConf
					, NoAbstractFsError, uri.GetScheme()));
			}
			return (Org.Apache.Hadoop.FS.AbstractFileSystem)NewInstance(clazz, uri, conf);
		}

		/// <summary>Get the statistics for a particular file system.</summary>
		/// <param name="uri">
		/// used as key to lookup STATISTICS_TABLE. Only scheme and authority
		/// part of the uri are used.
		/// </param>
		/// <returns>a statistics object</returns>
		protected internal static FileSystem.Statistics GetStatistics(URI uri)
		{
			lock (typeof(AbstractFileSystem))
			{
				string scheme = uri.GetScheme();
				if (scheme == null)
				{
					throw new ArgumentException("Scheme not defined in the uri: " + uri);
				}
				URI baseUri = GetBaseUri(uri);
				FileSystem.Statistics result = StatisticsTable[baseUri];
				if (result == null)
				{
					result = new FileSystem.Statistics(scheme);
					StatisticsTable[baseUri] = result;
				}
				return result;
			}
		}

		private static URI GetBaseUri(URI uri)
		{
			string scheme = uri.GetScheme();
			string authority = uri.GetAuthority();
			string baseUriString = scheme + "://";
			if (authority != null)
			{
				baseUriString = baseUriString + authority;
			}
			else
			{
				baseUriString = baseUriString + "/";
			}
			return URI.Create(baseUriString);
		}

		public static void ClearStatistics()
		{
			lock (typeof(AbstractFileSystem))
			{
				foreach (FileSystem.Statistics stat in StatisticsTable.Values)
				{
					stat.Reset();
				}
			}
		}

		/// <summary>Prints statistics for all file systems.</summary>
		public static void PrintStatistics()
		{
			lock (typeof(AbstractFileSystem))
			{
				foreach (KeyValuePair<URI, FileSystem.Statistics> pair in StatisticsTable)
				{
					System.Console.Out.WriteLine("  FileSystem " + pair.Key.GetScheme() + "://" + pair
						.Key.GetAuthority() + ": " + pair.Value);
				}
			}
		}

		protected internal static IDictionary<URI, FileSystem.Statistics> GetAllStatistics
			()
		{
			lock (typeof(AbstractFileSystem))
			{
				IDictionary<URI, FileSystem.Statistics> statsMap = new Dictionary<URI, FileSystem.Statistics
					>(StatisticsTable.Count);
				foreach (KeyValuePair<URI, FileSystem.Statistics> pair in StatisticsTable)
				{
					URI key = pair.Key;
					FileSystem.Statistics value = pair.Value;
					FileSystem.Statistics newStatsObj = new FileSystem.Statistics(value);
					statsMap[URI.Create(key.ToString())] = newStatsObj;
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
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public static Org.Apache.Hadoop.FS.AbstractFileSystem Get(URI uri, Configuration 
			conf)
		{
			return CreateFileSystem(uri, conf);
		}

		/// <summary>Constructor to be called by subclasses.</summary>
		/// <param name="uri">for this file system.</param>
		/// <param name="supportedScheme">the scheme supported by the implementor</param>
		/// <param name="authorityNeeded">
		/// if true then theURI must have authority, if false
		/// then the URI must have null authority.
		/// </param>
		/// <exception cref="Sharpen.URISyntaxException"><code>uri</code> has syntax error</exception>
		public AbstractFileSystem(URI uri, string supportedScheme, bool authorityNeeded, 
			int defaultPort)
		{
			myUri = GetUri(uri, supportedScheme, authorityNeeded, defaultPort);
			statistics = GetStatistics(uri);
		}

		/// <summary>Check that the Uri's scheme matches</summary>
		/// <param name="uri"/>
		/// <param name="supportedScheme"/>
		public virtual void CheckScheme(URI uri, string supportedScheme)
		{
			string scheme = uri.GetScheme();
			if (scheme == null)
			{
				throw new HadoopIllegalArgumentException("Uri without scheme: " + uri);
			}
			if (!scheme.Equals(supportedScheme))
			{
				throw new HadoopIllegalArgumentException("Uri scheme " + uri + " does not match the scheme "
					 + supportedScheme);
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
		/// <exception cref="Sharpen.URISyntaxException"><code>uri</code> has syntax error</exception>
		private URI GetUri(URI uri, string supportedScheme, bool authorityNeeded, int defaultPort
			)
		{
			CheckScheme(uri, supportedScheme);
			// A file system implementation that requires authority must always
			// specify default port
			if (defaultPort < 0 && authorityNeeded)
			{
				throw new HadoopIllegalArgumentException("FileSystem implementation error -  default port "
					 + defaultPort + " is not valid");
			}
			string authority = uri.GetAuthority();
			if (authority == null)
			{
				if (authorityNeeded)
				{
					throw new HadoopIllegalArgumentException("Uri without authority: " + uri);
				}
				else
				{
					return new URI(supportedScheme + ":///");
				}
			}
			// authority is non null  - AuthorityNeeded may be true or false.
			int port = uri.GetPort();
			port = (port == -1 ? defaultPort : port);
			if (port == -1)
			{
				// no port supplied and default port is not specified
				return new URI(supportedScheme, authority, "/", null);
			}
			return new URI(supportedScheme + "://" + uri.GetHost() + ":" + port);
		}

		/// <summary>The default port of this file system.</summary>
		/// <returns>
		/// default port of this file system's Uri scheme
		/// A uri with a port of -1 =&gt; default port;
		/// </returns>
		public abstract int GetUriDefaultPort();

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		/// <returns>the uri of this file system.</returns>
		public virtual URI GetUri()
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
		public virtual void CheckPath(Path path)
		{
			URI uri = path.ToUri();
			string thatScheme = uri.GetScheme();
			string thatAuthority = uri.GetAuthority();
			if (thatScheme == null)
			{
				if (thatAuthority == null)
				{
					if (path.IsUriPathAbsolute())
					{
						return;
					}
					throw new InvalidPathException("relative paths not allowed:" + path);
				}
				else
				{
					throw new InvalidPathException("Path without scheme with non-null authority:" + path
						);
				}
			}
			string thisScheme = this.GetUri().GetScheme();
			string thisHost = this.GetUri().GetHost();
			string thatHost = uri.GetHost();
			// Schemes and hosts must match.
			// Allow for null Authority for file:///
			if (!Sharpen.Runtime.EqualsIgnoreCase(thisScheme, thatScheme) || (thisHost != null
				 && !Sharpen.Runtime.EqualsIgnoreCase(thisHost, thatHost)) || (thisHost == null 
				&& thatHost != null))
			{
				throw new InvalidPathException("Wrong FS: " + path + ", expected: " + this.GetUri
					());
			}
			// Ports must match, unless this FS instance is using the default port, in
			// which case the port may be omitted from the given URI
			int thisPort = this.GetUri().GetPort();
			int thatPort = uri.GetPort();
			if (thatPort == -1)
			{
				// -1 => defaultPort of Uri scheme
				thatPort = this.GetUriDefaultPort();
			}
			if (thisPort != thatPort)
			{
				throw new InvalidPathException("Wrong FS: " + path + ", expected: " + this.GetUri
					());
			}
		}

		/// <summary>Get the path-part of a pathname.</summary>
		/// <remarks>
		/// Get the path-part of a pathname. Checks that URI matches this file system
		/// and that the path-part is a valid name.
		/// </remarks>
		/// <param name="p">path</param>
		/// <returns>path-part of the Path p</returns>
		public virtual string GetUriPath(Path p)
		{
			CheckPath(p);
			string s = p.ToUri().GetPath();
			if (!IsValidName(s))
			{
				throw new InvalidPathException("Path part " + s + " from URI " + p + " is not a valid filename."
					);
			}
			return s;
		}

		/// <summary>Make the path fully qualified to this file system</summary>
		/// <param name="path"/>
		/// <returns>the qualified path</returns>
		public virtual Path MakeQualified(Path path)
		{
			CheckPath(path);
			return path.MakeQualified(this.GetUri(), null);
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
		public virtual Path GetInitialWorkingDirectory()
		{
			return null;
		}

		/// <summary>Return the current user's home directory in this file system.</summary>
		/// <remarks>
		/// Return the current user's home directory in this file system.
		/// The default implementation returns "/user/$USER/".
		/// </remarks>
		/// <returns>current user's home directory.</returns>
		public virtual Path GetHomeDirectory()
		{
			return new Path("/user/" + Runtime.GetProperty("user.name")).MakeQualified(GetUri
				(), null);
		}

		/// <summary>Return a set of server default configuration values.</summary>
		/// <returns>server default configuration values</returns>
		/// <exception cref="System.IO.IOException">an I/O error occurred</exception>
		public abstract FsServerDefaults GetServerDefaults();

		/// <summary>
		/// Return the fully-qualified path of path f resolving the path
		/// through any internal symlinks or mount point
		/// </summary>
		/// <param name="p">path to be resolved</param>
		/// <returns>fully qualified path</returns>
		/// <exception cref="System.IO.FileNotFoundException">
		/// , AccessControlException, IOException
		/// UnresolvedLinkException if symbolic link on path cannot be resolved
		/// internally
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path ResolvePath(Path p)
		{
			CheckPath(p);
			return GetFileStatus(p).GetPath();
		}

		// default impl is to return the path
		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Create(Path, Sharpen.EnumSet{E}, CreateOpts[])"/>
		/// except
		/// that the Path f must be fully qualified and the permission is absolute
		/// (i.e. umask has been applied).
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public FSDataOutputStream Create(Path f, EnumSet<CreateFlag> createFlag, params Options.CreateOpts
			[] opts)
		{
			CheckPath(f);
			int bufferSize = -1;
			short replication = -1;
			long blockSize = -1;
			int bytesPerChecksum = -1;
			Options.ChecksumOpt checksumOpt = null;
			FsPermission permission = null;
			Progressable progress = null;
			bool createParent = null;
			foreach (Options.CreateOpts iOpt in opts)
			{
				if (typeof(Options.CreateOpts.BlockSize).IsInstanceOfType(iOpt))
				{
					if (blockSize != -1)
					{
						throw new HadoopIllegalArgumentException("BlockSize option is set multiple times"
							);
					}
					blockSize = ((Options.CreateOpts.BlockSize)iOpt).GetValue();
				}
				else
				{
					if (typeof(Options.CreateOpts.BufferSize).IsInstanceOfType(iOpt))
					{
						if (bufferSize != -1)
						{
							throw new HadoopIllegalArgumentException("BufferSize option is set multiple times"
								);
						}
						bufferSize = ((Options.CreateOpts.BufferSize)iOpt).GetValue();
					}
					else
					{
						if (typeof(Options.CreateOpts.ReplicationFactor).IsInstanceOfType(iOpt))
						{
							if (replication != -1)
							{
								throw new HadoopIllegalArgumentException("ReplicationFactor option is set multiple times"
									);
							}
							replication = ((Options.CreateOpts.ReplicationFactor)iOpt).GetValue();
						}
						else
						{
							if (typeof(Options.CreateOpts.BytesPerChecksum).IsInstanceOfType(iOpt))
							{
								if (bytesPerChecksum != -1)
								{
									throw new HadoopIllegalArgumentException("BytesPerChecksum option is set multiple times"
										);
								}
								bytesPerChecksum = ((Options.CreateOpts.BytesPerChecksum)iOpt).GetValue();
							}
							else
							{
								if (typeof(Options.CreateOpts.ChecksumParam).IsInstanceOfType(iOpt))
								{
									if (checksumOpt != null)
									{
										throw new HadoopIllegalArgumentException("CreateChecksumType option is set multiple times"
											);
									}
									checksumOpt = ((Options.CreateOpts.ChecksumParam)iOpt).GetValue();
								}
								else
								{
									if (typeof(Options.CreateOpts.Perms).IsInstanceOfType(iOpt))
									{
										if (permission != null)
										{
											throw new HadoopIllegalArgumentException("Perms option is set multiple times");
										}
										permission = ((Options.CreateOpts.Perms)iOpt).GetValue();
									}
									else
									{
										if (typeof(Options.CreateOpts.Progress).IsInstanceOfType(iOpt))
										{
											if (progress != null)
											{
												throw new HadoopIllegalArgumentException("Progress option is set multiple times");
											}
											progress = ((Options.CreateOpts.Progress)iOpt).GetValue();
										}
										else
										{
											if (typeof(Options.CreateOpts.CreateParent).IsInstanceOfType(iOpt))
											{
												if (createParent != null)
												{
													throw new HadoopIllegalArgumentException("CreateParent option is set multiple times"
														);
												}
												createParent = ((Options.CreateOpts.CreateParent)iOpt).GetValue();
											}
											else
											{
												throw new HadoopIllegalArgumentException("Unkown CreateOpts of type " + iOpt.GetType
													().FullName);
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
				throw new HadoopIllegalArgumentException("no permission supplied");
			}
			FsServerDefaults ssDef = GetServerDefaults();
			if (ssDef.GetBlockSize() % ssDef.GetBytesPerChecksum() != 0)
			{
				throw new IOException("Internal error: default blockSize is" + " not a multiple of default bytesPerChecksum "
					);
			}
			if (blockSize == -1)
			{
				blockSize = ssDef.GetBlockSize();
			}
			// Create a checksum option honoring user input as much as possible.
			// If bytesPerChecksum is specified, it will override the one set in
			// checksumOpt. Any missing value will be filled in using the default.
			Options.ChecksumOpt defaultOpt = new Options.ChecksumOpt(ssDef.GetChecksumType(), 
				ssDef.GetBytesPerChecksum());
			checksumOpt = Options.ChecksumOpt.ProcessChecksumOpt(defaultOpt, checksumOpt, bytesPerChecksum
				);
			if (bufferSize == -1)
			{
				bufferSize = ssDef.GetFileBufferSize();
			}
			if (replication == -1)
			{
				replication = ssDef.GetReplication();
			}
			if (createParent == null)
			{
				createParent = false;
			}
			if (blockSize % bytesPerChecksum != 0)
			{
				throw new HadoopIllegalArgumentException("blockSize should be a multiple of checksumsize"
					);
			}
			return this.CreateInternal(f, createFlag, permission, bufferSize, replication, blockSize
				, progress, checksumOpt, createParent);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="Create(Path, Sharpen.EnumSet{E}, CreateOpts[])"/>
		/// except that the opts
		/// have been declared explicitly.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Mkdir(Path, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)
		/// 	"/>
		/// except that the Path
		/// f must be fully qualified and the permission is absolute (i.e.
		/// umask has been applied).
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Mkdir(Path dir, FsPermission permission, bool createParent);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Delete(Path, bool)"/>
		/// except that Path f must be for
		/// this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool Delete(Path f, bool recursive);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Open(Path)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataInputStream Open(Path f)
		{
			return Open(f, GetServerDefaults().GetFileBufferSize());
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Open(Path, int)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract FSDataInputStream Open(Path f, int bufferSize);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Truncate(Path, long)"/>
		/// except that Path f must be for
		/// this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Truncate(Path f, long newLength)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support truncate");
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.SetReplication(Path, short)"/>
		/// except that Path f must be
		/// for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool SetReplication(Path f, short replication);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Rename(Path, Path, Rename[])"/>
		/// except that Path
		/// f must be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public void Rename(Path src, Path dst, params Options.Rename[] options)
		{
			bool overwrite = false;
			if (null != options)
			{
				foreach (Options.Rename option in options)
				{
					if (option == Options.Rename.Overwrite)
					{
						overwrite = true;
					}
				}
			}
			RenameInternal(src, dst, overwrite);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Rename(Path, Path, Rename[])"/>
		/// except that Path
		/// f must be for this file system and NO OVERWRITE is performed.
		/// File systems that do not have a built in overwrite need implement only this
		/// method and can take advantage of the default impl of the other
		/// <see cref="RenameInternal(Path, Path, bool)"/>
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RenameInternal(Path src, Path dst);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Rename(Path, Path, Rename[])"/>
		/// except that Path
		/// f must be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RenameInternal(Path src, Path dst, bool overwrite)
		{
			// Default implementation deals with overwrite in a non-atomic way
			FileStatus srcStatus = GetFileLinkStatus(src);
			FileStatus dstStatus;
			try
			{
				dstStatus = GetFileLinkStatus(dst);
			}
			catch (IOException)
			{
				dstStatus = null;
			}
			if (dstStatus != null)
			{
				if (dst.Equals(src))
				{
					throw new FileAlreadyExistsException("The source " + src + " and destination " + 
						dst + " are the same");
				}
				if (srcStatus.IsSymlink() && dst.Equals(srcStatus.GetSymlink()))
				{
					throw new FileAlreadyExistsException("Cannot rename symlink " + src + " to its target "
						 + dst);
				}
				// It's OK to rename a file to a symlink and vice versa
				if (srcStatus.IsDirectory() != dstStatus.IsDirectory())
				{
					throw new IOException("Source " + src + " and destination " + dst + " must both be directories"
						);
				}
				if (!overwrite)
				{
					throw new FileAlreadyExistsException("Rename destination " + dst + " already exists."
						);
				}
				// Delete the destination that is a file or an empty directory
				if (dstStatus.IsDirectory())
				{
					RemoteIterator<FileStatus> list = ListStatusIterator(dst);
					if (list != null && list.HasNext())
					{
						throw new IOException("Rename cannot overwrite non empty destination directory " 
							+ dst);
					}
				}
				Delete(dst, false);
			}
			else
			{
				Path parent = dst.GetParent();
				FileStatus parentStatus = GetFileStatus(parent);
				if (parentStatus.IsFile())
				{
					throw new ParentNotDirectoryException("Rename destination parent " + parent + " is a file."
						);
				}
			}
			RenameInternal(src, dst);
		}

		/// <summary>Returns true if the file system supports symlinks, false otherwise.</summary>
		/// <returns>true if filesystem supports symlinks</returns>
		public virtual bool SupportsSymlinks()
		{
			return false;
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.CreateSymlink(Path, Path, bool)"/>
		/// ;
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual void CreateSymlink(Path target, Path link, bool createParent)
		{
			throw new IOException("File system does not support symlinks");
		}

		/// <summary>Partially resolves the path.</summary>
		/// <remarks>
		/// Partially resolves the path. This is used during symlink resolution in
		/// <see cref="FSLinkResolver{T}"/>
		/// , and differs from the similarly named method
		/// <see cref="FileContext.GetLinkTarget(Path)"/>
		/// .
		/// </remarks>
		/// <exception cref="System.IO.IOException">subclass implementations may throw IOException
		/// 	</exception>
		public virtual Path GetLinkTarget(Path f)
		{
			throw new Exception("Implementation Error: " + GetType() + " that threw an UnresolvedLinkException, causing this method to be"
				 + " called, needs to override this method.");
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		/// except that Path f
		/// must be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetPermission(Path f, FsPermission permission);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.SetOwner(Path, string, string)"/>
		/// except that Path f must
		/// be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetOwner(Path f, string username, string groupname);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.SetTimes(Path, long, long)"/>
		/// except that Path f must be
		/// for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetTimes(Path f, long mtime, long atime);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.GetFileChecksum(Path)"/>
		/// except that Path f must be for
		/// this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract FileChecksum GetFileChecksum(Path f);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.GetFileStatus(Path)"/>
		/// 
		/// except that an UnresolvedLinkException may be thrown if a symlink is
		/// encountered in the path.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract FileStatus GetFileStatus(Path f);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.Access(Path, Org.Apache.Hadoop.FS.Permission.FsAction)"/>
		/// except that an UnresolvedLinkException may be thrown if a symlink is
		/// encountered in the path.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Access(Path path, FsAction mode)
		{
			FileSystem.CheckAccessPermissions(this.GetFileStatus(path), mode);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.GetFileLinkStatus(Path)"/>
		/// except that an UnresolvedLinkException may be thrown if a symlink is
		/// encountered in the path leading up to the final path component.
		/// If the file system does not support symlinks then the behavior is
		/// equivalent to
		/// <see cref="GetFileStatus(Path)"/>
		/// .
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus GetFileLinkStatus(Path f)
		{
			return GetFileStatus(f);
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.GetFileBlockLocations(Path, long, long)"/>
		/// except that
		/// Path f must be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			);

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.GetFsStatus(Path)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FsStatus GetFsStatus(Path f)
		{
			// default impl gets FsStatus of root
			return GetFsStatus();
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.GetFsStatus(Path)"/>
		/// .
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract FsStatus GetFsStatus();

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.ListStatus(Path)"/>
		/// except that Path f must be for this
		/// file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<FileStatus> ListStatusIterator(Path f)
		{
			return new _RemoteIterator_887(this, f);
		}

		private sealed class _RemoteIterator_887 : RemoteIterator<FileStatus>
		{
			public _RemoteIterator_887(Path f)
			{
				this.f = f;
				this.i = 0;
				this.statusList = this._enclosing.ListStatus(f);
			}

			private int i;

			private FileStatus[] statusList;

			public bool HasNext()
			{
				return this.i < this.statusList.Length;
			}

			public FileStatus Next()
			{
				if (!this.HasNext())
				{
					throw new NoSuchElementException();
				}
				return this.statusList[this.i++];
			}

			private readonly Path f;
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.ListLocatedStatus(Path)"/>
		/// except that Path f
		/// must be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path f)
		{
			return new _RemoteIterator_914(this, f);
		}

		private sealed class _RemoteIterator_914 : RemoteIterator<LocatedFileStatus>
		{
			public _RemoteIterator_914(AbstractFileSystem _enclosing, Path f)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.itor = this._enclosing.ListStatusIterator(f);
			}

			private RemoteIterator<FileStatus> itor;

			/// <exception cref="System.IO.IOException"/>
			public bool HasNext()
			{
				return this.itor.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public LocatedFileStatus Next()
			{
				if (!this.HasNext())
				{
					throw new NoSuchElementException("No more entry in " + f);
				}
				FileStatus result = this.itor.Next();
				BlockLocation[] locs = null;
				if (result.IsFile())
				{
					locs = this._enclosing.GetFileBlockLocations(result.GetPath(), 0, result.GetLen()
						);
				}
				return new LocatedFileStatus(result, locs);
			}

			private readonly AbstractFileSystem _enclosing;

			private readonly Path f;
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="Util.ListStatus(Path)"/>
		/// except that Path f must be
		/// for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract FileStatus[] ListStatus(Path f);

		/// <returns>
		/// an iterator over the corrupt files under the given path
		/// (may contain duplicates if a file has more than one corrupt block)
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<Path> ListCorruptFileBlocks(Path path)
		{
			throw new NotSupportedException(GetType().GetCanonicalName() + " does not support"
				 + " listCorruptFileBlocks");
		}

		/// <summary>
		/// The specification of this method matches that of
		/// <see cref="FileContext.SetVerifyChecksum(bool, Path)"/>
		/// except that Path f
		/// must be for this file system.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetVerifyChecksum(bool verifyChecksum);

		/// <summary>Get a canonical name for this file system.</summary>
		/// <returns>a URI string that uniquely identifies this file system</returns>
		public virtual string GetCanonicalServiceName()
		{
			return SecurityUtil.BuildDTServiceName(GetUri(), GetUriDefaultPort());
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
		public virtual IList<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			(string renewer)
		{
			return new AList<Org.Apache.Hadoop.Security.Token.Token<object>>(0);
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
		public virtual void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support modifyAclEntries"
				);
		}

		/// <summary>Removes ACL entries from files and directories.</summary>
		/// <remarks>
		/// Removes ACL entries from files and directories.  Other ACL entries are
		/// retained.
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">List<AclEntry> describing entries to remove</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support removeAclEntries"
				);
		}

		/// <summary>Removes all default ACL entries from files and directories.</summary>
		/// <param name="path">Path to modify</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void RemoveDefaultAcl(Path path)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support removeDefaultAcl"
				);
		}

		/// <summary>Removes all but the base ACL entries of files and directories.</summary>
		/// <remarks>
		/// Removes all but the base ACL entries of files and directories.  The entries
		/// for user, group, and others are retained for compatibility with permission
		/// bits.
		/// </remarks>
		/// <param name="path">Path to modify</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be removed</exception>
		public virtual void RemoveAcl(Path path)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support removeAcl");
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
		public virtual void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support setAcl");
		}

		/// <summary>Gets the ACLs of files and directories.</summary>
		/// <param name="path">Path to get</param>
		/// <returns>RemoteIterator<AclStatus> which returns each AclStatus</returns>
		/// <exception cref="System.IO.IOException">if an ACL could not be read</exception>
		public virtual AclStatus GetAclStatus(Path path)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getAclStatus");
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
		public virtual void SetXAttr(Path path, string name, byte[] value)
		{
			SetXAttr(path, name, value, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace)
				);
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
		public virtual void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support setXAttr");
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
		public virtual byte[] GetXAttr(Path path, string name)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getXAttr");
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
		public virtual IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getXAttrs");
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
		public virtual IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getXAttrs");
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
		public virtual IList<string> ListXAttrs(Path path)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support listXAttrs");
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
		public virtual void RemoveXAttr(Path path, string name)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support removeXAttr");
		}

		public override int GetHashCode()
		{
			//Object
			return myUri.GetHashCode();
		}

		public override bool Equals(object other)
		{
			//Object
			if (other == null || !(other is Org.Apache.Hadoop.FS.AbstractFileSystem))
			{
				return false;
			}
			return myUri.Equals(((Org.Apache.Hadoop.FS.AbstractFileSystem)other).myUri);
		}
	}
}

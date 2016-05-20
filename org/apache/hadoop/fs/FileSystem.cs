using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>An abstract base class for a fairly generic filesystem.</summary>
	/// <remarks>
	/// An abstract base class for a fairly generic filesystem.  It
	/// may be implemented as a distributed filesystem, or as a "local"
	/// one that reflects the locally-connected disk.  The local version
	/// exists for small Hadoop instances and for testing.
	/// <p>
	/// All user code that may potentially use the Hadoop Distributed
	/// File System should be written to use a FileSystem object.  The
	/// Hadoop DFS is a multi-machine system that appears as a single
	/// disk.  It's useful because of its fault tolerance and potentially
	/// very large capacity.
	/// <p>
	/// The local implementation is
	/// <see cref="LocalFileSystem"/>
	/// and distributed
	/// implementation is DistributedFileSystem.
	/// </remarks>
	public abstract class FileSystem : org.apache.hadoop.conf.Configured, java.io.Closeable
	{
		public const string FS_DEFAULT_NAME_KEY = org.apache.hadoop.fs.CommonConfigurationKeys
			.FS_DEFAULT_NAME_KEY;

		public const string DEFAULT_FS = org.apache.hadoop.fs.CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT;

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem))
			);

		/// <summary>Priority of the FileSystem shutdown hook.</summary>
		public const int SHUTDOWN_HOOK_PRIORITY = 10;

		/// <summary>FileSystem cache</summary>
		internal static readonly org.apache.hadoop.fs.FileSystem.Cache CACHE = new org.apache.hadoop.fs.FileSystem.Cache
			();

		/// <summary>The key this instance is stored under in the cache.</summary>
		private org.apache.hadoop.fs.FileSystem.Cache.Key key;

		/// <summary>Recording statistics per a FileSystem class</summary>
		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.fs.FileSystem.Statistics
			> statisticsTable = new java.util.IdentityHashMap<java.lang.Class, org.apache.hadoop.fs.FileSystem.Statistics
			>();

		/// <summary>The statistics for this file system.</summary>
		protected internal org.apache.hadoop.fs.FileSystem.Statistics statistics;

		/// <summary>
		/// A cache of files that should be deleted when filsystem is closed
		/// or the JVM is exited.
		/// </summary>
		private System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path> deleteOnExit
			 = new java.util.TreeSet<org.apache.hadoop.fs.Path>();

		internal bool resolveSymlinks;

		/// <summary>This method adds a file system for testing so that we can find it later.
		/// 	</summary>
		/// <remarks>
		/// This method adds a file system for testing so that we can find it later. It
		/// is only for testing.
		/// </remarks>
		/// <param name="uri">the uri to store it under</param>
		/// <param name="conf">the configuration to store it under</param>
		/// <param name="fs">the file system to store</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void addFileSystemForTesting(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.FileSystem fs)
		{
			CACHE.map[new org.apache.hadoop.fs.FileSystem.Cache.Key(uri, conf)] = fs;
		}

		/// <summary>
		/// Get a filesystem instance based on the uri, the passed
		/// configuration and the user
		/// </summary>
		/// <param name="uri">of the filesystem</param>
		/// <param name="conf">the configuration to use</param>
		/// <param name="user">to perform the get as</param>
		/// <returns>the filesystem instance</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.fs.FileSystem get(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf, string user)
		{
			string ticketCachePath = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH
				);
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getBestUGI(ticketCachePath, user);
			return ugi.doAs(new _PrivilegedExceptionAction_157(uri, conf));
		}

		private sealed class _PrivilegedExceptionAction_157 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_157(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(uri, conf);
			}

			private readonly java.net.URI uri;

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <summary>Returns the configured filesystem implementation.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FileSystem get(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return get(getDefaultUri(conf), conf);
		}

		/// <summary>Get the default filesystem URI from a configuration.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <returns>the uri of the default filesystem</returns>
		public static java.net.URI getDefaultUri(org.apache.hadoop.conf.Configuration conf
			)
		{
			return java.net.URI.create(fixName(conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)));
		}

		/// <summary>Set the default filesystem URI in a configuration.</summary>
		/// <param name="conf">the configuration to alter</param>
		/// <param name="uri">the new default filesystem uri</param>
		public static void setDefaultUri(org.apache.hadoop.conf.Configuration conf, java.net.URI
			 uri)
		{
			conf.set(FS_DEFAULT_NAME_KEY, uri.ToString());
		}

		/// <summary>Set the default filesystem URI in a configuration.</summary>
		/// <param name="conf">the configuration to alter</param>
		/// <param name="uri">the new default filesystem uri</param>
		public static void setDefaultUri(org.apache.hadoop.conf.Configuration conf, string
			 uri)
		{
			setDefaultUri(conf, java.net.URI.create(fixName(uri)));
		}

		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="name">
		/// a uri whose authority section names the host, port, etc.
		/// for this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void initialize(java.net.URI name, org.apache.hadoop.conf.Configuration
			 conf)
		{
			statistics = getStatistics(name.getScheme(), Sharpen.Runtime.getClassForObject(this
				));
			resolveSymlinks = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT
				);
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// This implementation throws an <code>UnsupportedOperationException</code>.
		/// </remarks>
		/// <returns>the protocol scheme for the FileSystem.</returns>
		public virtual string getScheme()
		{
			throw new System.NotSupportedException("Not implemented by the " + Sharpen.Runtime.getClassForObject
				(this).getSimpleName() + " FileSystem implementation");
		}

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		public abstract java.net.URI getUri();

		/// <summary>Return a canonicalized form of this FileSystem's URI.</summary>
		/// <remarks>
		/// Return a canonicalized form of this FileSystem's URI.
		/// The default implementation simply calls
		/// <see cref="canonicalizeUri(java.net.URI)"/>
		/// on the filesystem's own URI, so subclasses typically only need to
		/// implement that method.
		/// </remarks>
		/// <seealso cref="canonicalizeUri(java.net.URI)"/>
		protected internal virtual java.net.URI getCanonicalUri()
		{
			return canonicalizeUri(getUri());
		}

		/// <summary>Canonicalize the given URI.</summary>
		/// <remarks>
		/// Canonicalize the given URI.
		/// This is filesystem-dependent, but may for example consist of
		/// canonicalizing the hostname using DNS and adding the default
		/// port if not specified.
		/// The default implementation simply fills in the default port if
		/// not specified and if the filesystem has a default port.
		/// </remarks>
		/// <returns>URI</returns>
		/// <seealso cref="org.apache.hadoop.net.NetUtils.getCanonicalUri(java.net.URI, int)"
		/// 	/>
		protected internal virtual java.net.URI canonicalizeUri(java.net.URI uri)
		{
			if (uri.getPort() == -1 && getDefaultPort() > 0)
			{
				// reconstruct the uri with the default port set
				try
				{
					uri = new java.net.URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), getDefaultPort
						(), uri.getPath(), uri.getQuery(), uri.getFragment());
				}
				catch (java.net.URISyntaxException)
				{
					// Should never happen!
					throw new java.lang.AssertionError("Valid URI became unparseable: " + uri);
				}
			}
			return uri;
		}

		/// <summary>Get the default port for this file system.</summary>
		/// <returns>the default port or 0 if there isn't one</returns>
		protected internal virtual int getDefaultPort()
		{
			return 0;
		}

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal static org.apache.hadoop.fs.FileSystem getFSofPath(org.apache.hadoop.fs.Path
			 absOrFqPath, org.apache.hadoop.conf.Configuration conf)
		{
			absOrFqPath.checkNotSchemeWithRelative();
			absOrFqPath.checkNotRelative();
			// Uses the default file system if not fully qualified
			return get(absOrFqPath.toUri(), conf);
		}

		/// <summary>Get a canonical service name for this file system.</summary>
		/// <remarks>
		/// Get a canonical service name for this file system.  The token cache is
		/// the only user of the canonical service name, and uses it to lookup this
		/// filesystem's service tokens.
		/// If file system provides a token of its own then it must have a canonical
		/// name, otherwise canonical name can be null.
		/// Default Impl: If the file system has child file systems
		/// (such as an embedded file system) then it is assumed that the fs has no
		/// tokens of its own and hence returns a null name; otherwise a service
		/// name is built using Uri and port.
		/// </remarks>
		/// <returns>
		/// a service string that uniquely identifies this file system, null
		/// if the filesystem does not implement tokens
		/// </returns>
		/// <seealso cref="org.apache.hadoop.security.SecurityUtil.buildDTServiceName(java.net.URI, int)
		/// 	"></seealso>
		public virtual string getCanonicalServiceName()
		{
			return (getChildFileSystems() == null) ? org.apache.hadoop.security.SecurityUtil.
				buildDTServiceName(getUri(), getDefaultPort()) : null;
		}

		[System.ObsoleteAttribute(@"call #getUri() instead.")]
		public virtual string getName()
		{
			return getUri().ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"call #get(URI,Configuration) instead.")]
		public static org.apache.hadoop.fs.FileSystem getNamed(string name, org.apache.hadoop.conf.Configuration
			 conf)
		{
			return get(java.net.URI.create(fixName(name)), conf);
		}

		/// <summary>Update old-format filesystem names, for back-compatibility.</summary>
		/// <remarks>
		/// Update old-format filesystem names, for back-compatibility.  This should
		/// eventually be replaced with a checkName() method that throws an exception
		/// for old-format names.
		/// </remarks>
		private static string fixName(string name)
		{
			// convert old-format name to new-format name
			if (name.Equals("local"))
			{
				// "local" is now "file:///".
				LOG.warn("\"local\" is a deprecated filesystem name." + " Use \"file:///\" instead."
					);
				name = "file:///";
			}
			else
			{
				if (name.IndexOf('/') == -1)
				{
					// unqualified is "hdfs://"
					LOG.warn("\"" + name + "\" is a deprecated filesystem name." + " Use \"hdfs://" +
						 name + "/\" instead.");
					name = "hdfs://" + name;
				}
			}
			return name;
		}

		/// <summary>Get the local file system.</summary>
		/// <param name="conf">the configuration to configure the file system with</param>
		/// <returns>a LocalFileSystem</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.LocalFileSystem getLocal(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return (org.apache.hadoop.fs.LocalFileSystem)get(org.apache.hadoop.fs.LocalFileSystem
				.NAME, conf);
		}

		/// <summary>Returns the FileSystem for this URI's scheme and authority.</summary>
		/// <remarks>
		/// Returns the FileSystem for this URI's scheme and authority.  The scheme
		/// of the URI determines a configuration property name,
		/// <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
		/// The entire URI is passed to the FileSystem instance's initialize method.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FileSystem get(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			string scheme = uri.getScheme();
			string authority = uri.getAuthority();
			if (scheme == null && authority == null)
			{
				// use default FS
				return get(conf);
			}
			if (scheme != null && authority == null)
			{
				// no authority
				java.net.URI defaultUri = getDefaultUri(conf);
				if (scheme.Equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null)
				{
					// if scheme matches default
					// & default has authority
					return get(defaultUri, conf);
				}
			}
			// return default
			string disableCacheName = string.format("fs.%s.impl.disable.cache", scheme);
			if (conf.getBoolean(disableCacheName, false))
			{
				return createFileSystem(uri, conf);
			}
			return CACHE.get(uri, conf);
		}

		/// <summary>
		/// Returns the FileSystem for this URI's scheme and authority and the
		/// passed user.
		/// </summary>
		/// <remarks>
		/// Returns the FileSystem for this URI's scheme and authority and the
		/// passed user. Internally invokes
		/// <see cref="newInstance(java.net.URI, org.apache.hadoop.conf.Configuration)"/>
		/// </remarks>
		/// <param name="uri">of the filesystem</param>
		/// <param name="conf">the configuration to use</param>
		/// <param name="user">to perform the get as</param>
		/// <returns>filesystem instance</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.fs.FileSystem newInstance(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf, string user)
		{
			string ticketCachePath = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH
				);
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getBestUGI(ticketCachePath, user);
			return ugi.doAs(new _PrivilegedExceptionAction_390(uri, conf));
		}

		private sealed class _PrivilegedExceptionAction_390 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_390(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.newInstance(uri, conf);
			}

			private readonly java.net.URI uri;

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <summary>Returns the FileSystem for this URI's scheme and authority.</summary>
		/// <remarks>
		/// Returns the FileSystem for this URI's scheme and authority.  The scheme
		/// of the URI determines a configuration property name,
		/// <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
		/// The entire URI is passed to the FileSystem instance's initialize method.
		/// This always returns a new FileSystem object.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FileSystem newInstance(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			string scheme = uri.getScheme();
			string authority = uri.getAuthority();
			if (scheme == null)
			{
				// no scheme: use default FS
				return newInstance(conf);
			}
			if (authority == null)
			{
				// no authority
				java.net.URI defaultUri = getDefaultUri(conf);
				if (scheme.Equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null)
				{
					// if scheme matches default
					// & default has authority
					return newInstance(defaultUri, conf);
				}
			}
			// return default
			return CACHE.getUnique(uri, conf);
		}

		/// <summary>Returns a unique configured filesystem implementation.</summary>
		/// <remarks>
		/// Returns a unique configured filesystem implementation.
		/// This always returns a new FileSystem object.
		/// </remarks>
		/// <param name="conf">the configuration to use</param>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FileSystem newInstance(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return newInstance(getDefaultUri(conf), conf);
		}

		/// <summary>Get a unique local file system object</summary>
		/// <param name="conf">the configuration to configure the file system with</param>
		/// <returns>
		/// a LocalFileSystem
		/// This always returns a new FileSystem object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.LocalFileSystem newInstanceLocal(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return (org.apache.hadoop.fs.LocalFileSystem)newInstance(org.apache.hadoop.fs.LocalFileSystem
				.NAME, conf);
		}

		/// <summary>Close all cached filesystems.</summary>
		/// <remarks>
		/// Close all cached filesystems. Be sure those filesystems are not
		/// used anymore.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void closeAll()
		{
			CACHE.closeAll();
		}

		/// <summary>Close all cached filesystems for a given UGI.</summary>
		/// <remarks>
		/// Close all cached filesystems for a given UGI. Be sure those filesystems
		/// are not used anymore.
		/// </remarks>
		/// <param name="ugi">user group info to close</param>
		/// <exception cref="System.IO.IOException"/>
		public static void closeAllForUGI(org.apache.hadoop.security.UserGroupInformation
			 ugi)
		{
			CACHE.closeAll(ugi);
		}

		/// <summary>Make sure that a path specifies a FileSystem.</summary>
		/// <param name="path">to use</param>
		public virtual org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path 
			path)
		{
			checkPath(path);
			return path.makeQualified(this.getUri(), this.getWorkingDirectory());
		}

		/// <summary>Get a new delegation token for this file system.</summary>
		/// <remarks>
		/// Get a new delegation token for this file system.
		/// This is an internal method that should have been declared protected
		/// but wasn't historically.
		/// Callers should use
		/// <see cref="addDelegationTokens(string, org.apache.hadoop.security.Credentials)"/>
		/// </remarks>
		/// <param name="renewer">the account name that is allowed to renew the token.</param>
		/// <returns>a new delegation token</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.security.token.Token<object> getDelegationToken(
			string renewer)
		{
			return null;
		}

		/// <summary>
		/// Obtain all delegation tokens used by this FileSystem that are not
		/// already present in the given Credentials.
		/// </summary>
		/// <remarks>
		/// Obtain all delegation tokens used by this FileSystem that are not
		/// already present in the given Credentials.  Existing tokens will neither
		/// be verified as valid nor having the given renewer.  Missing tokens will
		/// be acquired and added to the given Credentials.
		/// Default Impl: works for simple fs with its own token
		/// and also for an embedded fs whose tokens are those of its
		/// children file system (i.e. the embedded fs has not tokens of its
		/// own).
		/// </remarks>
		/// <param name="renewer">the user allowed to renew the delegation tokens</param>
		/// <param name="credentials">cache in which to add new delegation tokens</param>
		/// <returns>list of new delegation tokens</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
			(string renewer, org.apache.hadoop.security.Credentials credentials)
		{
			if (credentials == null)
			{
				credentials = new org.apache.hadoop.security.Credentials();
			}
			System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
				tokens = new System.Collections.Generic.List<org.apache.hadoop.security.token.Token
				<object>>();
			collectDelegationTokens(renewer, credentials, tokens);
			return Sharpen.Collections.ToArray(tokens, new org.apache.hadoop.security.token.Token
				<object>[tokens.Count]);
		}

		/// <summary>
		/// Recursively obtain the tokens for this FileSystem and all descended
		/// FileSystems as determined by getChildFileSystems().
		/// </summary>
		/// <param name="renewer">the user allowed to renew the delegation tokens</param>
		/// <param name="credentials">cache in which to add the new delegation tokens</param>
		/// <param name="tokens">list in which to add acquired tokens</param>
		/// <exception cref="System.IO.IOException"/>
		private void collectDelegationTokens(string renewer, org.apache.hadoop.security.Credentials
			 credentials, System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> tokens)
		{
			string serviceName = getCanonicalServiceName();
			// Collect token of the this filesystem and then of its embedded children
			if (serviceName != null)
			{
				// fs has token, grab it
				org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text(serviceName);
				org.apache.hadoop.security.token.Token<object> token = credentials.getToken(service
					);
				if (token == null)
				{
					token = getDelegationToken(renewer);
					if (token != null)
					{
						tokens.add(token);
						credentials.addToken(service, token);
					}
				}
			}
			// Now collect the tokens from the children
			org.apache.hadoop.fs.FileSystem[] children = getChildFileSystems();
			if (children != null)
			{
				foreach (org.apache.hadoop.fs.FileSystem fs in children)
				{
					fs.collectDelegationTokens(renewer, credentials, tokens);
				}
			}
		}

		/// <summary>Get all the immediate child FileSystems embedded in this FileSystem.</summary>
		/// <remarks>
		/// Get all the immediate child FileSystems embedded in this FileSystem.
		/// It does not recurse and get grand children.  If a FileSystem
		/// has multiple child FileSystems, then it should return a unique list
		/// of those FileSystems.  Default is to return null to signify no children.
		/// </remarks>
		/// <returns>FileSystems used by this FileSystem</returns>
		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.fs.FileSystem[] getChildFileSystems()
		{
			return null;
		}

		/// <summary>
		/// create a file with the provided permission
		/// The permission of the file is set to be the provided permission as in
		/// setPermission, not permission&~umask
		/// It is implemented using two RPCs.
		/// </summary>
		/// <remarks>
		/// create a file with the provided permission
		/// The permission of the file is set to be the provided permission as in
		/// setPermission, not permission&~umask
		/// It is implemented using two RPCs. It is understood that it is inefficient,
		/// but the implementation is thread-safe. The other option is to change the
		/// value of umask in configuration to be 0, but it is not thread-safe.
		/// </remarks>
		/// <param name="fs">file system handle</param>
		/// <param name="file">the name of the file to be created</param>
		/// <param name="permission">the permission of the file</param>
		/// <returns>an output stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path file, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			// create the file with default permission
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(file);
			// set its permission to the supplied one
			fs.setPermission(file, permission);
			return @out;
		}

		/// <summary>
		/// create a directory with the provided permission
		/// The permission of the directory is set to be the provided permission as in
		/// setPermission, not permission&~umask
		/// </summary>
		/// <seealso cref="create(FileSystem, Path, org.apache.hadoop.fs.permission.FsPermission)
		/// 	"/>
		/// <param name="fs">file system handle</param>
		/// <param name="dir">the name of the directory to be created</param>
		/// <param name="permission">the permission of the directory</param>
		/// <returns>true if the directory creation succeeds; false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool mkdirs(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 dir, org.apache.hadoop.fs.permission.FsPermission permission)
		{
			// create the directory using the default permission
			bool result = fs.mkdirs(dir);
			// set its permission to be the supplied one
			fs.setPermission(dir, permission);
			return result;
		}

		protected internal FileSystem()
			: base(null)
		{
		}

		///////////////////////////////////////////////////////////////
		// FileSystem
		///////////////////////////////////////////////////////////////
		/// <summary>Check that a Path belongs to this FileSystem.</summary>
		/// <param name="path">to check</param>
		protected internal virtual void checkPath(org.apache.hadoop.fs.Path path)
		{
			java.net.URI uri = path.toUri();
			string thatScheme = uri.getScheme();
			if (thatScheme == null)
			{
				// fs is relative
				return;
			}
			java.net.URI thisUri = getCanonicalUri();
			string thisScheme = thisUri.getScheme();
			//authority and scheme are not case sensitive
			if (Sharpen.Runtime.equalsIgnoreCase(thisScheme, thatScheme))
			{
				// schemes match
				string thisAuthority = thisUri.getAuthority();
				string thatAuthority = uri.getAuthority();
				if (thatAuthority == null && thisAuthority != null)
				{
					// path's authority is null
					// fs has an authority
					java.net.URI defaultUri = getDefaultUri(getConf());
					if (Sharpen.Runtime.equalsIgnoreCase(thisScheme, defaultUri.getScheme()))
					{
						uri = defaultUri;
					}
					else
					{
						// schemes match, so use this uri instead
						uri = null;
					}
				}
				// can't determine auth of the path
				if (uri != null)
				{
					// canonicalize uri before comparing with this fs
					uri = canonicalizeUri(uri);
					thatAuthority = uri.getAuthority();
					if (thisAuthority == thatAuthority || (thisAuthority != null && Sharpen.Runtime.equalsIgnoreCase
						(thisAuthority, thatAuthority)))
					{
						// authorities match
						return;
					}
				}
			}
			throw new System.ArgumentException("Wrong FS: " + path + ", expected: " + this.getUri
				());
		}

		/// <summary>
		/// Return an array containing hostnames, offset and size of
		/// portions of the given file.
		/// </summary>
		/// <remarks>
		/// Return an array containing hostnames, offset and size of
		/// portions of the given file.  For a nonexistent
		/// file or regions, null will be returned.
		/// This call is most helpful with DFS, where it returns
		/// hostnames of machines that contain the given file.
		/// The FileSystem will simply return an elt containing 'localhost'.
		/// </remarks>
		/// <param name="file">FilesStatus to get data from</param>
		/// <param name="start">offset into the given file</param>
		/// <param name="len">length for which to get locations for</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus
			 file, long start, long len)
		{
			if (file == null)
			{
				return null;
			}
			if (start < 0 || len < 0)
			{
				throw new System.ArgumentException("Invalid start or len parameter");
			}
			if (file.getLen() <= start)
			{
				return new org.apache.hadoop.fs.BlockLocation[0];
			}
			string[] name = new string[] { "localhost:50010" };
			string[] host = new string[] { "localhost" };
			return new org.apache.hadoop.fs.BlockLocation[] { new org.apache.hadoop.fs.BlockLocation
				(name, host, 0, file.getLen()) };
		}

		/// <summary>
		/// Return an array containing hostnames, offset and size of
		/// portions of the given file.
		/// </summary>
		/// <remarks>
		/// Return an array containing hostnames, offset and size of
		/// portions of the given file.  For a nonexistent
		/// file or regions, null will be returned.
		/// This call is most helpful with DFS, where it returns
		/// hostnames of machines that contain the given file.
		/// The FileSystem will simply return an elt containing 'localhost'.
		/// </remarks>
		/// <param name="p">
		/// path is used to identify an FS since an FS could have
		/// another FS that it could be delegating the call to
		/// </param>
		/// <param name="start">offset into the given file</param>
		/// <param name="len">length for which to get locations for</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 p, long start, long len)
		{
			if (p == null)
			{
				throw new System.ArgumentNullException();
			}
			org.apache.hadoop.fs.FileStatus file = getFileStatus(p);
			return getFileBlockLocations(file, start, len);
		}

		/// <summary>Return a set of server default configuration values</summary>
		/// <returns>server default configuration values</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"use getServerDefaults(Path) instead")]
		public virtual org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			org.apache.hadoop.conf.Configuration conf = getConf();
			// CRC32 is chosen as default as it is available in all 
			// releases that support checksum.
			// The client trash configuration is ignored.
			return new org.apache.hadoop.fs.FsServerDefaults(getDefaultBlockSize(), conf.getInt
				("io.bytes.per.checksum", 512), 64 * 1024, getDefaultReplication(), conf.getInt(
				"io.file.buffer.size", 4096), false, org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.FS_TRASH_INTERVAL_DEFAULT, org.apache.hadoop.util.DataChecksum.Type.CRC32);
		}

		/// <summary>Return a set of server default configuration values</summary>
		/// <param name="p">
		/// path is used to identify an FS since an FS could have
		/// another FS that it could be delegating the call to
		/// </param>
		/// <returns>server default configuration values</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path
			 p)
		{
			return getServerDefaults();
		}

		/// <summary>
		/// Return the fully-qualified path of path f resolving the path
		/// through any symlinks or mount point
		/// </summary>
		/// <param name="p">path to be resolved</param>
		/// <returns>fully qualified path</returns>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p)
		{
			checkPath(p);
			return getFileStatus(p).getPath();
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize);

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file to open</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f)
		{
			return open(f, getConf().getInt("io.file.buffer.size", 4096));
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <remarks>
		/// Create an FSDataOutputStream at the indicated Path.
		/// Files are overwritten by default.
		/// </remarks>
		/// <param name="f">the file to create</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f)
		{
			return create(f, true);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <param name="f">the file to create</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an exception will be thrown.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, bool overwrite)
		{
			return create(f, overwrite, getConf().getInt("io.file.buffer.size", 4096), getDefaultReplication
				(f), getDefaultBlockSize(f));
		}

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <remarks>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// Files are overwritten by default.
		/// </remarks>
		/// <param name="f">the file to create</param>
		/// <param name="progress">to report progress</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.util.Progressable progress)
		{
			return create(f, true, getConf().getInt("io.file.buffer.size", 4096), getDefaultReplication
				(f), getDefaultBlockSize(f), progress);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <remarks>
		/// Create an FSDataOutputStream at the indicated Path.
		/// Files are overwritten by default.
		/// </remarks>
		/// <param name="f">the file to create</param>
		/// <param name="replication">the replication factor</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, short replication)
		{
			return create(f, true, getConf().getInt("io.file.buffer.size", 4096), replication
				, getDefaultBlockSize(f));
		}

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <remarks>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// Files are overwritten by default.
		/// </remarks>
		/// <param name="f">the file to create</param>
		/// <param name="replication">the replication factor</param>
		/// <param name="progress">to report progress</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, short replication, org.apache.hadoop.util.Progressable progress)
		{
			return create(f, true, getConf().getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.IO_FILE_BUFFER_SIZE_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT
				), replication, getDefaultBlockSize(f), progress);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <param name="f">the file name to create</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize)
		{
			return create(f, overwrite, bufferSize, getDefaultReplication(f), getDefaultBlockSize
				(f));
		}

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <param name="f">the path of the file to open</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize, org.apache.hadoop.util.Progressable progress
			)
		{
			return create(f, overwrite, bufferSize, getDefaultReplication(f), getDefaultBlockSize
				(f), progress);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize, short replication, long blockSize)
		{
			return create(f, overwrite, bufferSize, replication, blockSize, null);
		}

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <param name="f">the file name to open</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return this.create(f, org.apache.hadoop.fs.permission.FsPermission.getFileDefault
				().applyUMask(org.apache.hadoop.fs.permission.FsPermission.getUMask(getConf())), 
				overwrite, bufferSize, replication, blockSize, progress);
		}

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <param name="f">the file name to open</param>
		/// <param name="permission"/>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize"/>
		/// <param name="progress"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)"
		/// 	/>
		public abstract org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress);

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <param name="f">the file name to open</param>
		/// <param name="permission"/>
		/// <param name="flags">
		/// 
		/// <see cref="CreateFlag"/>
		/// s to use for this stream.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize"/>
		/// <param name="progress"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)"
		/// 	/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return create(f, permission, flags, bufferSize, replication, blockSize, progress, 
				null);
		}

		/// <summary>
		/// Create an FSDataOutputStream at the indicated Path with a custom
		/// checksum option
		/// </summary>
		/// <param name="f">the file name to open</param>
		/// <param name="permission"/>
		/// <param name="flags">
		/// 
		/// <see cref="CreateFlag"/>
		/// s to use for this stream.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize"/>
		/// <param name="progress"/>
		/// <param name="checksumOpt">
		/// checksum parameter. If null, the values
		/// found in conf will be used.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)"
		/// 	/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt)
		{
			// Checksum options are ignored by default. The file systems that
			// implement checksum need to override this method. The full
			// support is currently only available in DFS.
			return create(f, permission, flags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE
				), bufferSize, replication, blockSize, progress);
		}

		/*.
		* This create has been added to support the FileContext that processes
		* the permission
		* with umask before calling this method.
		* This a temporary method added to support the transition from FileSystem
		* to FileContext for user applications.
		*/
		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		protected internal virtual org.apache.hadoop.fs.FSDataOutputStream primitiveCreate
			(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission absolutePermission
			, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, int bufferSize, short
			 replication, long blockSize, org.apache.hadoop.util.Progressable progress, org.apache.hadoop.fs.Options.ChecksumOpt
			 checksumOpt)
		{
			bool pathExists = exists(f);
			org.apache.hadoop.fs.CreateFlag.validate(f, pathExists, flag);
			// Default impl  assumes that permissions do not matter and 
			// nor does the bytesPerChecksum  hence
			// calling the regular create is good enough.
			// FSs that implement permissions should override this.
			if (pathExists && flag.contains(org.apache.hadoop.fs.CreateFlag.APPEND))
			{
				return append(f, bufferSize, progress);
			}
			return this.create(f, absolutePermission, flag.contains(org.apache.hadoop.fs.CreateFlag
				.OVERWRITE), bufferSize, replication, blockSize, progress);
		}

		/// <summary>This version of the mkdirs method assumes that the permission is absolute.
		/// 	</summary>
		/// <remarks>
		/// This version of the mkdirs method assumes that the permission is absolute.
		/// It has been added to support the FileContext that processes the permission
		/// with umask before calling this method.
		/// This a temporary method added to support the transition from FileSystem
		/// to FileContext for user applications.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		protected internal virtual bool primitiveMkdir(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission)
		{
			// Default impl is to assume that permissions do not matter and hence
			// calling the regular mkdirs is good enough.
			// FSs that implement permissions should override this.
			return this.mkdirs(f, absolutePermission);
		}

		/// <summary>This version of the mkdirs method assumes that the permission is absolute.
		/// 	</summary>
		/// <remarks>
		/// This version of the mkdirs method assumes that the permission is absolute.
		/// It has been added to support the FileContext that processes the permission
		/// with umask before calling this method.
		/// This a temporary method added to support the transition from FileSystem
		/// to FileContext for user applications.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		protected internal virtual void primitiveMkdir(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, bool createParent)
		{
			if (!createParent)
			{
				// parent must exist.
				// since the this.mkdirs makes parent dirs automatically
				// we must throw exception if parent does not exist.
				org.apache.hadoop.fs.FileStatus stat = getFileStatus(f.getParent());
				if (stat == null)
				{
					throw new java.io.FileNotFoundException("Missing parent:" + f);
				}
				if (!stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.ParentNotDirectoryException("parent is not a dir");
				}
			}
			// parent does exist - go ahead with mkdir of leaf
			// Default impl is to assume that permissions do not matter and hence
			// calling the regular mkdirs is good enough.
			// FSs that implement permissions should override this.
			if (!this.mkdirs(f, absolutePermission))
			{
				throw new System.IO.IOException("mkdir of " + f + " failed");
			}
		}

		/// <summary>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <remarks>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting. Same as create(), except fails if parent directory doesn't
		/// already exist.
		/// </remarks>
		/// <param name="f">the file name to open</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize"/>
		/// <param name="progress"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)"
		/// 	/>
		[System.ObsoleteAttribute(@"API only for 0.20-append")]
		public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return this.createNonRecursive(f, org.apache.hadoop.fs.permission.FsPermission.getFileDefault
				(), overwrite, bufferSize, replication, blockSize, progress);
		}

		/// <summary>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <remarks>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting. Same as create(), except fails if parent directory doesn't
		/// already exist.
		/// </remarks>
		/// <param name="f">the file name to open</param>
		/// <param name="permission"/>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize"/>
		/// <param name="progress"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)"
		/// 	/>
		[System.ObsoleteAttribute(@"API only for 0.20-append")]
		public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return createNonRecursive(f, permission, overwrite ? java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag
				.CREATE, org.apache.hadoop.fs.CreateFlag.OVERWRITE) : java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag
				.CREATE), bufferSize, replication, blockSize, progress);
		}

		/// <summary>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <remarks>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting. Same as create(), except fails if parent directory doesn't
		/// already exist.
		/// </remarks>
		/// <param name="f">the file name to open</param>
		/// <param name="permission"/>
		/// <param name="flags">
		/// 
		/// <see cref="CreateFlag"/>
		/// s to use for this stream.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize"/>
		/// <param name="progress"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)"
		/// 	/>
		[System.ObsoleteAttribute(@"API only for 0.20-append")]
		public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			throw new System.IO.IOException("createNonRecursive unsupported for this filesystem "
				 + Sharpen.Runtime.getClassForObject(this));
		}

		/// <summary>Creates the given Path as a brand-new zero-length file.</summary>
		/// <remarks>
		/// Creates the given Path as a brand-new zero-length file.  If
		/// create fails, or if it already existed, return false.
		/// </remarks>
		/// <param name="f">path to use for create</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool createNewFile(org.apache.hadoop.fs.Path f)
		{
			if (exists(f))
			{
				return false;
			}
			else
			{
				create(f, false, getConf().getInt("io.file.buffer.size", 4096)).close();
				return true;
			}
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <remarks>
		/// Append to an existing file (optional operation).
		/// Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
		/// </remarks>
		/// <param name="f">the existing file to be appended.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f)
		{
			return append(f, getConf().getInt("io.file.buffer.size", 4096), null);
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <remarks>
		/// Append to an existing file (optional operation).
		/// Same as append(f, bufferSize, null).
		/// </remarks>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			return append(f, bufferSize, null);
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="progress">for reporting progress if it is not null.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress);

		/// <summary>Concat existing files together.</summary>
		/// <param name="trg">the path to the target destination.</param>
		/// <param name="psrcs">the paths to the sources to use for the concatenation.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void concat(org.apache.hadoop.fs.Path trg, org.apache.hadoop.fs.Path
			[] psrcs)
		{
			throw new System.NotSupportedException("Not implemented by the " + Sharpen.Runtime.getClassForObject
				(this).getSimpleName() + " FileSystem implementation");
		}

		/// <summary>Get replication.</summary>
		/// <param name="src">file name</param>
		/// <returns>file replication</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use getFileStatus() instead")]
		public virtual short getReplication(org.apache.hadoop.fs.Path src)
		{
			return getFileStatus(src).getReplication();
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		public virtual bool setReplication(org.apache.hadoop.fs.Path src, short replication
			)
		{
			return true;
		}

		/// <summary>Renames Path src to Path dst.</summary>
		/// <remarks>
		/// Renames Path src to Path dst.  Can take place on local fs
		/// or remote DFS.
		/// </remarks>
		/// <param name="src">path to be renamed</param>
		/// <param name="dst">new path after rename</param>
		/// <exception cref="System.IO.IOException">on failure</exception>
		/// <returns>true if rename is successful</returns>
		public abstract bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst);

		/// <summary>
		/// Renames Path src to Path dst
		/// <ul>
		/// &lt;li
		/// <li>Fails if src is a file and dst is a directory.
		/// </summary>
		/// <remarks>
		/// Renames Path src to Path dst
		/// <ul>
		/// &lt;li
		/// <li>Fails if src is a file and dst is a directory.
		/// <li>Fails if src is a directory and dst is a file.
		/// <li>Fails if the parent of dst does not exist or is a file.
		/// </ul>
		/// <p>
		/// If OVERWRITE option is not passed as an argument, rename fails
		/// if the dst already exists.
		/// <p>
		/// If OVERWRITE option is passed as an argument, rename overwrites
		/// the dst if it is a file or an empty directory. Rename fails if dst is
		/// a non-empty directory.
		/// <p>
		/// Note that atomicity of rename is dependent on the file system
		/// implementation. Please refer to the file system documentation for
		/// details. This default implementation is non atomic.
		/// <p>
		/// This method is deprecated since it is a temporary method added to
		/// support the transition from FileSystem to FileContext for user
		/// applications.
		/// </remarks>
		/// <param name="src">path to be renamed</param>
		/// <param name="dst">new path after rename</param>
		/// <exception cref="System.IO.IOException">on failure</exception>
		[System.Obsolete]
		protected internal virtual void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, params org.apache.hadoop.fs.Options.Rename[] options)
		{
			// Default implementation
			org.apache.hadoop.fs.FileStatus srcStatus = getFileLinkStatus(src);
			if (srcStatus == null)
			{
				throw new java.io.FileNotFoundException("rename source " + src + " not found.");
			}
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
				if (srcStatus.isDirectory() != dstStatus.isDirectory())
				{
					throw new System.IO.IOException("Source " + src + " Destination " + dst + " both should be either file or directory"
						);
				}
				if (!overwrite)
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("rename destination " +
						 dst + " already exists.");
				}
				// Delete the destination that is a file or an empty directory
				if (dstStatus.isDirectory())
				{
					org.apache.hadoop.fs.FileStatus[] list = listStatus(dst);
					if (list != null && list.Length != 0)
					{
						throw new System.IO.IOException("rename cannot overwrite non empty destination directory "
							 + dst);
					}
				}
				delete(dst, false);
			}
			else
			{
				org.apache.hadoop.fs.Path parent = dst.getParent();
				org.apache.hadoop.fs.FileStatus parentStatus = getFileStatus(parent);
				if (parentStatus == null)
				{
					throw new java.io.FileNotFoundException("rename destination parent " + parent + " not found."
						);
				}
				if (!parentStatus.isDirectory())
				{
					throw new org.apache.hadoop.fs.ParentNotDirectoryException("rename destination parent "
						 + parent + " is a file.");
				}
			}
			if (!rename(src, dst))
			{
				throw new System.IO.IOException("rename from " + src + " to " + dst + " failed.");
			}
		}

		/// <summary>Truncate the file in the indicated path to the indicated size.</summary>
		/// <remarks>
		/// Truncate the file in the indicated path to the indicated size.
		/// <ul>
		/// <li>Fails if path is a directory.
		/// <li>Fails if path does not exist.
		/// <li>Fails if path is not closed.
		/// <li>Fails if new size is greater than current size.
		/// </ul>
		/// </remarks>
		/// <param name="f">The path to the file to be truncated</param>
		/// <param name="newLength">The size the file is to be truncated to</param>
		/// <returns>
		/// <code>true</code> if the file has been truncated to the desired
		/// <code>newLength</code> and is immediately available to be reused for
		/// write operations such as <code>append</code>, or
		/// <code>false</code> if a background process of adjusting the length of
		/// the last block has been started, and clients should wait for it to
		/// complete before proceeding with further file updates.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			throw new System.NotSupportedException("Not implemented by the " + Sharpen.Runtime.getClassForObject
				(this).getSimpleName() + " FileSystem implementation");
		}

		/// <summary>Delete a file</summary>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use delete(Path, bool) instead.")]
		public virtual bool delete(org.apache.hadoop.fs.Path f)
		{
			return delete(f, true);
		}

		/// <summary>Delete a file.</summary>
		/// <param name="f">the path to delete.</param>
		/// <param name="recursive">
		/// if path is a directory and set to
		/// true, the directory is deleted else throws an exception. In
		/// case of a file the recursive can be set to either true or false.
		/// </param>
		/// <returns>true if delete is successful else false.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool delete(org.apache.hadoop.fs.Path f, bool recursive);

		/// <summary>Mark a path to be deleted when FileSystem is closed.</summary>
		/// <remarks>
		/// Mark a path to be deleted when FileSystem is closed.
		/// When the JVM shuts down,
		/// all FileSystem objects will be closed automatically.
		/// Then,
		/// the marked path will be deleted as a result of closing the FileSystem.
		/// The path has to exist in the file system.
		/// </remarks>
		/// <param name="f">the path to delete.</param>
		/// <returns>true if deleteOnExit is successful, otherwise false.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool deleteOnExit(org.apache.hadoop.fs.Path f)
		{
			if (!exists(f))
			{
				return false;
			}
			lock (deleteOnExit)
			{
				deleteOnExit.add(f);
			}
			return true;
		}

		/// <summary>Cancel the deletion of the path when the FileSystem is closed</summary>
		/// <param name="f">the path to cancel deletion</param>
		public virtual bool cancelDeleteOnExit(org.apache.hadoop.fs.Path f)
		{
			lock (deleteOnExit)
			{
				return deleteOnExit.remove(f);
			}
		}

		/// <summary>Delete all files that were marked as delete-on-exit.</summary>
		/// <remarks>
		/// Delete all files that were marked as delete-on-exit. This recursively
		/// deletes all files in the specified paths.
		/// </remarks>
		protected internal virtual void processDeleteOnExit()
		{
			lock (deleteOnExit)
			{
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.Path> iter = deleteOnExit
					.GetEnumerator(); iter.MoveNext(); )
				{
					org.apache.hadoop.fs.Path path = iter.Current;
					try
					{
						if (exists(path))
						{
							delete(path, true);
						}
					}
					catch (System.IO.IOException)
					{
						LOG.info("Ignoring failure to deleteOnExit for path " + path);
					}
					iter.remove();
				}
			}
		}

		/// <summary>Check if exists.</summary>
		/// <param name="f">source file</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool exists(org.apache.hadoop.fs.Path f)
		{
			try
			{
				return getFileStatus(f) != null;
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <summary>True iff the named path is a directory.</summary>
		/// <remarks>
		/// True iff the named path is a directory.
		/// Note: Avoid using this method. Instead reuse the FileStatus
		/// returned by getFileStatus() or listStatus() methods.
		/// </remarks>
		/// <param name="f">path to check</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool isDirectory(org.apache.hadoop.fs.Path f)
		{
			try
			{
				return getFileStatus(f).isDirectory();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		// f does not exist
		/// <summary>True iff the named path is a regular file.</summary>
		/// <remarks>
		/// True iff the named path is a regular file.
		/// Note: Avoid using this method. Instead reuse the FileStatus
		/// returned by getFileStatus() or listStatus() methods.
		/// </remarks>
		/// <param name="f">path to check</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool isFile(org.apache.hadoop.fs.Path f)
		{
			try
			{
				return getFileStatus(f).isFile();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		// f does not exist
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use getFileStatus() instead")]
		public virtual long getLength(org.apache.hadoop.fs.Path f)
		{
			return getFileStatus(f).getLen();
		}

		/// <summary>
		/// Return the
		/// <see cref="ContentSummary"/>
		/// of a given
		/// <see cref="Path"/>
		/// .
		/// </summary>
		/// <param name="f">path to use</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.FileStatus status = getFileStatus(f);
			if (status.isFile())
			{
				// f is a file
				long length = status.getLen();
				return new org.apache.hadoop.fs.ContentSummary.Builder().length(length).fileCount
					(1).directoryCount(0).spaceConsumed(length).build();
			}
			// f is a directory
			long[] summary = new long[] { 0, 0, 1 };
			foreach (org.apache.hadoop.fs.FileStatus s in listStatus(f))
			{
				long length = s.getLen();
				org.apache.hadoop.fs.ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath
					()) : new org.apache.hadoop.fs.ContentSummary.Builder().length(length).fileCount
					(1).directoryCount(0).spaceConsumed(length).build();
				summary[0] += c.getLength();
				summary[1] += c.getFileCount();
				summary[2] += c.getDirectoryCount();
			}
			return new org.apache.hadoop.fs.ContentSummary.Builder().length(summary[0]).fileCount
				(summary[1]).directoryCount(summary[2]).spaceConsumed(summary[0]).build();
		}

		private sealed class _PathFilter_1490 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_1490()
			{
			}

			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return true;
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter DEFAULT_FILTER = new _PathFilter_1490
			();

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given patch</returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f);

		/*
		* Filter files/directories in the given path using the user-supplied path
		* filter. Results are added to the given array <code>results</code>.
		*/
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private void listStatus(System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus
			> results, org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter filter)
		{
			org.apache.hadoop.fs.FileStatus[] listing = listStatus(f);
			if (listing == null)
			{
				throw new System.IO.IOException("Error accessing " + f);
			}
			for (int i = 0; i < listing.Length; i++)
			{
				if (filter.accept(listing[i].getPath()))
				{
					results.add(listing[i]);
				}
			}
		}

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
		/// Filter files/directories in the given path using the user-supplied path
		/// filter.
		/// </summary>
		/// <param name="f">a path name</param>
		/// <param name="filter">the user-supplied path filter</param>
		/// <returns>
		/// an array of FileStatus objects for the files under the given path
		/// after applying the filter
		/// </returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.PathFilter filter)
		{
			System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> results = new System.Collections.Generic.List
				<org.apache.hadoop.fs.FileStatus>();
			listStatus(results, f, filter);
			return Sharpen.Collections.ToArray(results, new org.apache.hadoop.fs.FileStatus[results
				.Count]);
		}

		/// <summary>
		/// Filter files/directories in the given list of paths using default
		/// path filter.
		/// </summary>
		/// <param name="files">a list of paths</param>
		/// <returns>
		/// a list of statuses for the files under the given paths after
		/// applying the filter default Path filter
		/// </returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			[] files)
		{
			return listStatus(files, DEFAULT_FILTER);
		}

		/// <summary>
		/// Filter files/directories in the given list of paths using user-supplied
		/// path filter.
		/// </summary>
		/// <param name="files">a list of paths</param>
		/// <param name="filter">the user-supplied path filter</param>
		/// <returns>
		/// a list of statuses for the files under the given paths after
		/// applying the filter
		/// </returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			[] files, org.apache.hadoop.fs.PathFilter filter)
		{
			System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> results = new System.Collections.Generic.List
				<org.apache.hadoop.fs.FileStatus>();
			for (int i = 0; i < files.Length; i++)
			{
				listStatus(results, files[i], filter);
			}
			return Sharpen.Collections.ToArray(results, new org.apache.hadoop.fs.FileStatus[results
				.Count]);
		}

		/// <summary>
		/// <p>Return all the files that match filePattern and are not checksum
		/// files.
		/// </summary>
		/// <remarks>
		/// <p>Return all the files that match filePattern and are not checksum
		/// files. Results are sorted by their names.
		/// <p>
		/// A filename pattern is composed of <i>regular</i> characters and
		/// <i>special pattern matching</i> characters, which are:
		/// <dl>
		/// <dd>
		/// <dl>
		/// <p>
		/// <dt> <tt> ? </tt>
		/// <dd> Matches any single character.
		/// <p>
		/// <dt> <tt> * </tt>
		/// <dd> Matches zero or more characters.
		/// <p>
		/// <dt> <tt> [<i>abc</i>] </tt>
		/// <dd> Matches a single character from character set
		/// <tt>{<i>a,b,c</i>}</tt>.
		/// <p>
		/// <dt> <tt> [<i>a</i>-<i>b</i>] </tt>
		/// <dd> Matches a single character from the character range
		/// <tt>{<i>a...b</i>}</tt>.  Note that character <tt><i>a</i></tt> must be
		/// lexicographically less than or equal to character <tt><i>b</i></tt>.
		/// <p>
		/// <dt> <tt> [^<i>a</i>] </tt>
		/// <dd> Matches a single character that is not from character set or range
		/// <tt>{<i>a</i>}</tt>.  Note that the <tt>^</tt> character must occur
		/// immediately to the right of the opening bracket.
		/// <p>
		/// <dt> <tt> \<i>c</i> </tt>
		/// <dd> Removes (escapes) any special meaning of character <i>c</i>.
		/// <p>
		/// <dt> <tt> {ab,cd} </tt>
		/// <dd> Matches a string from the string set <tt>{<i>ab, cd</i>} </tt>
		/// <p>
		/// <dt> <tt> {ab,c{de,fh}} </tt>
		/// <dd> Matches a string from the string set <tt>{<i>ab, cde, cfh</i>}</tt>
		/// </dl>
		/// </dd>
		/// </dl>
		/// </remarks>
		/// <param name="pathPattern">a regular expression specifying a pth pattern</param>
		/// <returns>an array of paths that match the path pattern</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
			 pathPattern)
		{
			return new org.apache.hadoop.fs.Globber(this, pathPattern, DEFAULT_FILTER).glob();
		}

		/// <summary>
		/// Return an array of FileStatus objects whose path names match pathPattern
		/// and is accepted by the user-supplied path filter.
		/// </summary>
		/// <remarks>
		/// Return an array of FileStatus objects whose path names match pathPattern
		/// and is accepted by the user-supplied path filter. Results are sorted by
		/// their path names.
		/// Return null if pathPattern has no glob and the path does not exist.
		/// Return an empty array if pathPattern has a glob and no path matches it.
		/// </remarks>
		/// <param name="pathPattern">a regular expression specifying the path pattern</param>
		/// <param name="filter">a user-supplied path filter</param>
		/// <returns>an array of FileStatus objects</returns>
		/// <exception cref="System.IO.IOException">if any I/O error occurs when fetching file status
		/// 	</exception>
		public virtual org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
			 pathPattern, org.apache.hadoop.fs.PathFilter filter)
		{
			return new org.apache.hadoop.fs.Globber(this, pathPattern, filter).glob();
		}

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <remarks>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// Return the file's status and block locations If the path is a file.
		/// If a returned status is a file, it contains the file's block locations.
		/// </remarks>
		/// <param name="f">is the path</param>
		/// <returns>
		/// an iterator that traverses statuses of the files/directories
		/// in the given path
		/// </returns>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f)
		{
			return listLocatedStatus(f, DEFAULT_FILTER);
		}

		/// <summary>
		/// Listing a directory
		/// The returned results include its block location if it is a file
		/// The results are filtered by the given path filter
		/// </summary>
		/// <param name="f">a path</param>
		/// <param name="filter">a path filter</param>
		/// <returns>
		/// an iterator that traverses statuses of the files/directories
		/// in the given path
		/// </returns>
		/// <exception cref="java.io.FileNotFoundException">if <code>f</code> does not exist</exception>
		/// <exception cref="System.IO.IOException">if any I/O error occurred</exception>
		protected internal virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter
			 filter)
		{
			return new _RemoteIterator_1711(this, f, filter);
		}

		private sealed class _RemoteIterator_1711 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			>
		{
			public _RemoteIterator_1711(FileSystem _enclosing, org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter
				 filter)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.filter = filter;
				this.stats = this._enclosing.listStatus(f, filter);
				this.i = 0;
			}

			private readonly org.apache.hadoop.fs.FileStatus[] stats;

			private int i;

			public bool hasNext()
			{
				return this.i < this.stats.Length;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.LocatedFileStatus next()
			{
				if (!this.hasNext())
				{
					throw new java.util.NoSuchElementException("No more entry in " + f);
				}
				org.apache.hadoop.fs.FileStatus result = this.stats[this.i++];
				org.apache.hadoop.fs.BlockLocation[] locs = result.isFile() ? this._enclosing.getFileBlockLocations
					(result.getPath(), 0, result.getLen()) : null;
				return new org.apache.hadoop.fs.LocatedFileStatus(result, locs);
			}

			private readonly FileSystem _enclosing;

			private readonly org.apache.hadoop.fs.Path f;

			private readonly org.apache.hadoop.fs.PathFilter filter;
		}

		/// <summary>
		/// Returns a remote iterator so that followup calls are made on demand
		/// while consuming the entries.
		/// </summary>
		/// <remarks>
		/// Returns a remote iterator so that followup calls are made on demand
		/// while consuming the entries. Each file system implementation should
		/// override this method and provide a more efficient implementation, if
		/// possible.
		/// </remarks>
		/// <param name="p">target path</param>
		/// <returns>remote iterator</returns>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path p)
		{
			return new _RemoteIterator_1745(this, p);
		}

		private sealed class _RemoteIterator_1745 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			>
		{
			public _RemoteIterator_1745(org.apache.hadoop.fs.Path p)
			{
				this.p = p;
				this.stats = this._enclosing.listStatus(p);
				this.i = 0;
			}

			private readonly org.apache.hadoop.fs.FileStatus[] stats;

			private int i;

			public bool hasNext()
			{
				return this.i < this.stats.Length;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.FileStatus next()
			{
				if (!this.hasNext())
				{
					throw new java.util.NoSuchElementException("No more entry in " + p);
				}
				return this.stats[this.i++];
			}

			private readonly org.apache.hadoop.fs.Path p;
		}

		/// <summary>List the statuses and block locations of the files in the given path.</summary>
		/// <remarks>
		/// List the statuses and block locations of the files in the given path.
		/// If the path is a directory,
		/// if recursive is false, returns files in the directory;
		/// if recursive is true, return files in the subtree rooted at the path.
		/// If the path is a file, return the file's status and block locations.
		/// </remarks>
		/// <param name="f">is the path</param>
		/// <param name="recursive">if the subdirectories need to be traversed recursively</param>
		/// <returns>an iterator that traverses statuses of the files</returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listFiles(org.apache.hadoop.fs.Path f, bool recursive)
		{
			return new _RemoteIterator_1783(this, f, recursive);
		}

		private sealed class _RemoteIterator_1783 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			>
		{
			public _RemoteIterator_1783(FileSystem _enclosing, org.apache.hadoop.fs.Path f, bool
				 recursive)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.recursive = recursive;
				this.itors = new java.util.Stack<org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
					>>();
				this.curItor = this._enclosing.listLocatedStatus(f);
			}

			private java.util.Stack<org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
				>> itors;

			private org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
				> curItor;

			private org.apache.hadoop.fs.LocatedFileStatus curFile;

			/// <exception cref="System.IO.IOException"/>
			public bool hasNext()
			{
				while (this.curFile == null)
				{
					if (this.curItor.hasNext())
					{
						this.handleFileStat(this.curItor.next());
					}
					else
					{
						if (!this.itors.empty())
						{
							this.curItor = this.itors.pop();
						}
						else
						{
							return false;
						}
					}
				}
				return true;
			}

			/// <summary>Process the input stat.</summary>
			/// <remarks>
			/// Process the input stat.
			/// If it is a file, return the file stat.
			/// If it is a directory, traverse the directory if recursive is true;
			/// ignore it if recursive is false.
			/// </remarks>
			/// <param name="stat">input status</param>
			/// <exception cref="System.IO.IOException">if any IO error occurs</exception>
			private void handleFileStat(org.apache.hadoop.fs.LocatedFileStatus stat)
			{
				if (stat.isFile())
				{
					// file
					this.curFile = stat;
				}
				else
				{
					if (recursive)
					{
						// directory
						this.itors.push(this.curItor);
						this.curItor = this._enclosing.listLocatedStatus(stat.getPath());
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.LocatedFileStatus next()
			{
				if (this.hasNext())
				{
					org.apache.hadoop.fs.LocatedFileStatus result = this.curFile;
					this.curFile = null;
					return result;
				}
				throw new java.util.NoSuchElementException("No more entry in " + f);
			}

			private readonly FileSystem _enclosing;

			private readonly org.apache.hadoop.fs.Path f;

			private readonly bool recursive;
		}

		/// <summary>Return the current user's home directory in this filesystem.</summary>
		/// <remarks>
		/// Return the current user's home directory in this filesystem.
		/// The default implementation returns "/user/$USER/".
		/// </remarks>
		public virtual org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return this.makeQualified(new org.apache.hadoop.fs.Path("/user/" + Sharpen.Runtime
				.getProperty("user.name")));
		}

		/// <summary>Set the current working directory for the given file system.</summary>
		/// <remarks>
		/// Set the current working directory for the given file system. All relative
		/// paths will be resolved relative to it.
		/// </remarks>
		/// <param name="new_dir"/>
		public abstract void setWorkingDirectory(org.apache.hadoop.fs.Path new_dir);

		/// <summary>Get the current working directory for the given file system</summary>
		/// <returns>the directory pathname</returns>
		public abstract org.apache.hadoop.fs.Path getWorkingDirectory();

		/// <summary>
		/// Note: with the new FilesContext class, getWorkingDirectory()
		/// will be removed.
		/// </summary>
		/// <remarks>
		/// Note: with the new FilesContext class, getWorkingDirectory()
		/// will be removed.
		/// The working directory is implemented in FilesContext.
		/// Some file systems like LocalFileSystem have an initial workingDir
		/// that we use as the starting workingDir. For other file systems
		/// like HDFS there is no built in notion of an initial workingDir.
		/// </remarks>
		/// <returns>
		/// if there is built in notion of workingDir then it
		/// is returned; else a null is returned.
		/// </returns>
		protected internal virtual org.apache.hadoop.fs.Path getInitialWorkingDirectory()
		{
			return null;
		}

		/// <summary>
		/// Call
		/// <see cref="mkdirs(Path, org.apache.hadoop.fs.permission.FsPermission)"/>
		/// with default permission.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool mkdirs(org.apache.hadoop.fs.Path f)
		{
			return mkdirs(f, org.apache.hadoop.fs.permission.FsPermission.getDirDefault());
		}

		/// <summary>
		/// Make the given file and all non-existent parents into
		/// directories.
		/// </summary>
		/// <remarks>
		/// Make the given file and all non-existent parents into
		/// directories. Has the semantics of Unix 'mkdir -p'.
		/// Existence of the directory hierarchy is not an error.
		/// </remarks>
		/// <param name="f">path to create</param>
		/// <param name="permission">to apply to f</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission);

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name and the source is kept intact afterwards
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			copyFromLocalFile(false, src, dst);
		}

		/// <summary>The src files is on the local disk.</summary>
		/// <remarks>
		/// The src files is on the local disk.  Add it to FS at
		/// the given dst name, removing the source afterwards.
		/// </remarks>
		/// <param name="srcs">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void moveFromLocalFile(org.apache.hadoop.fs.Path[] srcs, org.apache.hadoop.fs.Path
			 dst)
		{
			copyFromLocalFile(true, true, srcs, dst);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name, removing the source afterwards.
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void moveFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			copyFromLocalFile(true, src, dst);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <param name="delSrc">whether to delete the src</param>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyFromLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, 
			org.apache.hadoop.fs.Path dst)
		{
			copyFromLocalFile(delSrc, true, src, dst);
		}

		/// <summary>The src files are on the local disk.</summary>
		/// <remarks>
		/// The src files are on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <param name="delSrc">whether to delete the src</param>
		/// <param name="overwrite">whether to overwrite an existing file</param>
		/// <param name="srcs">array of paths which are source</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyFromLocalFile(bool delSrc, bool overwrite, org.apache.hadoop.fs.Path
			[] srcs, org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.conf.Configuration conf = getConf();
			org.apache.hadoop.fs.FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite
				, conf);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <param name="delSrc">whether to delete the src</param>
		/// <param name="overwrite">whether to overwrite an existing file</param>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyFromLocalFile(bool delSrc, bool overwrite, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.conf.Configuration conf = getConf();
			org.apache.hadoop.fs.FileUtil.copy(getLocal(conf), src, this, dst, delSrc, overwrite
				, conf);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			copyToLocalFile(false, src, dst);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// Remove the source afterwards
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void moveToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			copyToLocalFile(true, src, dst);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// delSrc indicates if the src will be removed or not.
		/// </remarks>
		/// <param name="delSrc">whether to delete the src</param>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			copyToLocalFile(delSrc, src, dst, false);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk. Copy it from FS
		/// control to the local dst name. delSrc indicates if the src will be removed
		/// or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem
		/// as local file system or not. RawLocalFileSystem is non crc file system.So,
		/// It will not create any crc files at local.
		/// </remarks>
		/// <param name="delSrc">whether to delete the src</param>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <param name="useRawLocalFileSystem">whether to use RawLocalFileSystem as local file system or not.
		/// 	</param>
		/// <exception cref="System.IO.IOException">- if any IO error</exception>
		public virtual void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool useRawLocalFileSystem)
		{
			org.apache.hadoop.conf.Configuration conf = getConf();
			org.apache.hadoop.fs.FileSystem local = null;
			if (useRawLocalFileSystem)
			{
				local = getLocal(conf).getRawFileSystem();
			}
			else
			{
				local = getLocal(conf);
			}
			org.apache.hadoop.fs.FileUtil.copy(this, src, local, dst, delSrc, conf);
		}

		/// <summary>Returns a local File that the user can write output to.</summary>
		/// <remarks>
		/// Returns a local File that the user can write output to.  The caller
		/// provides both the eventual FS target name and the local working
		/// file.  If the FS is local, we write directly into the target.  If
		/// the FS is remote, we write into the tmp local area.
		/// </remarks>
		/// <param name="fsOutputFile">path of output file</param>
		/// <param name="tmpLocalFile">path of local tmp file</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path startLocalOutput(org.apache.hadoop.fs.Path
			 fsOutputFile, org.apache.hadoop.fs.Path tmpLocalFile)
		{
			return tmpLocalFile;
		}

		/// <summary>Called when we're all done writing to the target.</summary>
		/// <remarks>
		/// Called when we're all done writing to the target.  A local FS will
		/// do nothing, because we've written to exactly the right place.  A remote
		/// FS will copy the contents of tmpLocalFile to the correct target at
		/// fsOutputFile.
		/// </remarks>
		/// <param name="fsOutputFile">path of output file</param>
		/// <param name="tmpLocalFile">path to local tmp file</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void completeLocalOutput(org.apache.hadoop.fs.Path fsOutputFile, org.apache.hadoop.fs.Path
			 tmpLocalFile)
		{
			moveFromLocalFile(tmpLocalFile, fsOutputFile);
		}

		/// <summary>No more filesystem operations are needed.</summary>
		/// <remarks>
		/// No more filesystem operations are needed.  Will
		/// release any held locks.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			// delete all files that were marked as delete-on-exit.
			processDeleteOnExit();
			CACHE.remove(this.key, this);
		}

		/// <summary>Return the total size of all files in the filesystem.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long getUsed()
		{
			long used = 0;
			org.apache.hadoop.fs.FileStatus[] files = listStatus(new org.apache.hadoop.fs.Path
				("/"));
			foreach (org.apache.hadoop.fs.FileStatus file in files)
			{
				used += file.getLen();
			}
			return used;
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use getFileStatus() instead")]
		public virtual long getBlockSize(org.apache.hadoop.fs.Path f)
		{
			return getFileStatus(f).getBlockSize();
		}

		/// <summary>
		/// Return the number of bytes that large input files should be optimally
		/// be split into to minimize i/o time.
		/// </summary>
		[System.ObsoleteAttribute(@"use getDefaultBlockSize(Path) instead")]
		public virtual long getDefaultBlockSize()
		{
			// default to 32MB: large enough to minimize the impact of seeks
			return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
		}

		/// <summary>
		/// Return the number of bytes that large input files should be optimally
		/// be split into to minimize i/o time.
		/// </summary>
		/// <remarks>
		/// Return the number of bytes that large input files should be optimally
		/// be split into to minimize i/o time.  The given path will be used to
		/// locate the actual filesystem.  The full path does not have to exist.
		/// </remarks>
		/// <param name="f">path of file</param>
		/// <returns>the default block size for the path's filesystem</returns>
		public virtual long getDefaultBlockSize(org.apache.hadoop.fs.Path f)
		{
			return getDefaultBlockSize();
		}

		/// <summary>Get the default replication.</summary>
		[System.ObsoleteAttribute(@"use getDefaultReplication(Path) instead")]
		public virtual short getDefaultReplication()
		{
			return 1;
		}

		/// <summary>Get the default replication for a path.</summary>
		/// <remarks>
		/// Get the default replication for a path.   The given path will be used to
		/// locate the actual filesystem.  The full path does not have to exist.
		/// </remarks>
		/// <param name="path">of the file</param>
		/// <returns>default replication for the path's filesystem</returns>
		public virtual short getDefaultReplication(org.apache.hadoop.fs.Path path)
		{
			return getDefaultReplication();
		}

		/// <summary>Return a file status object that represents the path.</summary>
		/// <param name="f">The path we want information from</param>
		/// <returns>a FileStatus object</returns>
		/// <exception cref="java.io.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f);

		/// <summary>Checks if the user can access a path.</summary>
		/// <remarks>
		/// Checks if the user can access a path.  The mode specifies which access
		/// checks to perform.  If the requested permissions are granted, then the
		/// method returns normally.  If access is denied, then the method throws an
		/// <see cref="org.apache.hadoop.security.AccessControlException"/>
		/// .
		/// <p/>
		/// The default implementation of this method calls
		/// <see cref="getFileStatus(Path)"/>
		/// and checks the returned permissions against the requested permissions.
		/// Note that the getFileStatus call will be subject to authorization checks.
		/// Typically, this requires search (execute) permissions on each directory in
		/// the path's prefix, but this is implementation-defined.  Any file system
		/// that provides a richer authorization model (such as ACLs) may override the
		/// default implementation so that it checks against that model instead.
		/// <p>
		/// In general, applications should avoid using this method, due to the risk of
		/// time-of-check/time-of-use race conditions.  The permissions on a file may
		/// change immediately after the access call returns.  Most applications should
		/// prefer running specific file system actions as the desired user represented
		/// by a
		/// <see cref="org.apache.hadoop.security.UserGroupInformation"/>
		/// .
		/// </remarks>
		/// <param name="path">Path to check</param>
		/// <param name="mode">type of access to check</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">if the path does not exist</exception>
		/// <exception cref="System.IO.IOException">see specific implementation</exception>
		public virtual void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			checkAccessPermissions(this.getFileStatus(path), mode);
		}

		/// <summary>
		/// This method provides the default implementation of
		/// <see cref="access(Path, org.apache.hadoop.fs.permission.FsAction)"/>
		/// .
		/// </summary>
		/// <param name="stat">FileStatus to check</param>
		/// <param name="mode">type of access to check</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		internal static void checkAccessPermissions(org.apache.hadoop.fs.FileStatus stat, 
			org.apache.hadoop.fs.permission.FsAction mode)
		{
			org.apache.hadoop.fs.permission.FsPermission perm = stat.getPermission();
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			string user = ugi.getShortUserName();
			System.Collections.Generic.IList<string> groups = java.util.Arrays.asList(ugi.getGroupNames
				());
			if (user.Equals(stat.getOwner()))
			{
				if (perm.getUserAction().implies(mode))
				{
					return;
				}
			}
			else
			{
				if (groups.contains(stat.getGroup()))
				{
					if (perm.getGroupAction().implies(mode))
					{
						return;
					}
				}
				else
				{
					if (perm.getOtherAction().implies(mode))
					{
						return;
					}
				}
			}
			throw new org.apache.hadoop.security.AccessControlException(string.format("Permission denied: user=%s, path=\"%s\":%s:%s:%s%s"
				, user, stat.getPath(), stat.getOwner(), stat.getGroup(), stat.isDirectory() ? "d"
				 : "-", perm));
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.fixRelativePart(Path)"/>
		/// </summary>
		protected internal virtual org.apache.hadoop.fs.Path fixRelativePart(org.apache.hadoop.fs.Path
			 p)
		{
			if (p.isUriPathAbsolute())
			{
				return p;
			}
			else
			{
				return new org.apache.hadoop.fs.Path(getWorkingDirectory(), p);
			}
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.createSymlink(Path, Path, bool)"/>
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			// Supporting filesystems should override this method
			throw new System.NotSupportedException("Filesystem does not support symlinks!");
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.getFileLinkStatus(Path)"/>
		/// </summary>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			// Supporting filesystems should override this method
			return getFileStatus(f);
		}

		/// <summary>
		/// See
		/// <see cref="AbstractFileSystem.supportsSymlinks()"/>
		/// </summary>
		public virtual bool supportsSymlinks()
		{
			return false;
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.getLinkTarget(Path)"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path 
			f)
		{
			// Supporting filesystems should override this method
			throw new System.NotSupportedException("Filesystem does not support symlinks!");
		}

		/// <summary>
		/// See
		/// <see cref="AbstractFileSystem.getLinkTarget(Path)"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Path resolveLink(org.apache.hadoop.fs.Path
			 f)
		{
			// Supporting filesystems should override this method
			throw new System.NotSupportedException("Filesystem does not support symlinks!");
		}

		/// <summary>Get the checksum of a file.</summary>
		/// <param name="f">The file path</param>
		/// <returns>
		/// The file checksum.  The default return value is null,
		/// which indicates that no checksum algorithm is implemented
		/// in the corresponding FileSystem.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			return getFileChecksum(f, long.MaxValue);
		}

		/// <summary>
		/// Get the checksum of a file, from the beginning of the file till the
		/// specific length.
		/// </summary>
		/// <param name="f">The file path</param>
		/// <param name="length">The length of the file range for checksum calculation</param>
		/// <returns>The file checksum.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f, long length)
		{
			return null;
		}

		/// <summary>Set the verify checksum flag.</summary>
		/// <remarks>
		/// Set the verify checksum flag. This is only applicable if the
		/// corresponding FileSystem supports checksum. By default doesn't do anything.
		/// </remarks>
		/// <param name="verifyChecksum"/>
		public virtual void setVerifyChecksum(bool verifyChecksum)
		{
		}

		//doesn't do anything
		/// <summary>Set the write checksum flag.</summary>
		/// <remarks>
		/// Set the write checksum flag. This is only applicable if the
		/// corresponding FileSystem supports checksum. By default doesn't do anything.
		/// </remarks>
		/// <param name="writeChecksum"/>
		public virtual void setWriteChecksum(bool writeChecksum)
		{
		}

		//doesn't do anything
		/// <summary>
		/// Returns a status object describing the use and capacity of the
		/// file system.
		/// </summary>
		/// <remarks>
		/// Returns a status object describing the use and capacity of the
		/// file system. If the file system has multiple partitions, the
		/// use and capacity of the root partition is reflected.
		/// </remarks>
		/// <returns>a FsStatus object</returns>
		/// <exception cref="System.IO.IOException">see specific implementation</exception>
		public virtual org.apache.hadoop.fs.FsStatus getStatus()
		{
			return getStatus(null);
		}

		/// <summary>
		/// Returns a status object describing the use and capacity of the
		/// file system.
		/// </summary>
		/// <remarks>
		/// Returns a status object describing the use and capacity of the
		/// file system. If the file system has multiple partitions, the
		/// use and capacity of the partition pointed to by the specified
		/// path is reflected.
		/// </remarks>
		/// <param name="p">
		/// Path for which status should be obtained. null means
		/// the default partition.
		/// </param>
		/// <returns>a FsStatus object</returns>
		/// <exception cref="System.IO.IOException">see specific implementation</exception>
		public virtual org.apache.hadoop.fs.FsStatus getStatus(org.apache.hadoop.fs.Path 
			p)
		{
			return new org.apache.hadoop.fs.FsStatus(long.MaxValue, 0, long.MaxValue);
		}

		/// <summary>Set permission of a path.</summary>
		/// <param name="p"/>
		/// <param name="permission"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setPermission(org.apache.hadoop.fs.Path p, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
		}

		/// <summary>Set owner of a path (i.e.</summary>
		/// <remarks>
		/// Set owner of a path (i.e. a file or a directory).
		/// The parameters username and groupname cannot both be null.
		/// </remarks>
		/// <param name="p">The path</param>
		/// <param name="username">If it is null, the original username remains unchanged.</param>
		/// <param name="groupname">If it is null, the original groupname remains unchanged.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setOwner(org.apache.hadoop.fs.Path p, string username, string
			 groupname)
		{
		}

		/// <summary>Set access time of a file</summary>
		/// <param name="p">The path</param>
		/// <param name="mtime">
		/// Set the modification time of this file.
		/// The number of milliseconds since Jan 1, 1970.
		/// A value of -1 means that this call should not set modification time.
		/// </param>
		/// <param name="atime">
		/// Set the access time of this file.
		/// The number of milliseconds since Jan 1, 1970.
		/// A value of -1 means that this call should not set access time.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setTimes(org.apache.hadoop.fs.Path p, long mtime, long atime)
		{
		}

		/// <summary>Create a snapshot with a default name.</summary>
		/// <param name="path">The directory where snapshots will be taken.</param>
		/// <returns>the snapshot path.</returns>
		/// <exception cref="System.IO.IOException"/>
		public org.apache.hadoop.fs.Path createSnapshot(org.apache.hadoop.fs.Path path)
		{
			return createSnapshot(path, null);
		}

		/// <summary>Create a snapshot</summary>
		/// <param name="path">The directory where snapshots will be taken.</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <returns>the snapshot path.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path createSnapshot(org.apache.hadoop.fs.Path
			 path, string snapshotName)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support createSnapshot");
		}

		/// <summary>Rename a snapshot</summary>
		/// <param name="path">The directory path where the snapshot was taken</param>
		/// <param name="snapshotOldName">Old name of the snapshot</param>
		/// <param name="snapshotNewName">New name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void renameSnapshot(org.apache.hadoop.fs.Path path, string snapshotOldName
			, string snapshotNewName)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support renameSnapshot");
		}

		/// <summary>Delete a snapshot of a directory</summary>
		/// <param name="path">The directory that the to-be-deleted snapshot belongs to</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void deleteSnapshot(org.apache.hadoop.fs.Path path, string snapshotName
			)
		{
			throw new System.NotSupportedException(Sharpen.Runtime.getClassForObject(this).getSimpleName
				() + " doesn't support deleteSnapshot");
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

		/// <summary>Gets the ACL of a file or directory.</summary>
		/// <param name="path">Path to get</param>
		/// <returns>AclStatus describing the ACL of the file or directory</returns>
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

		/// <summary>Get an xattr name and value for a file or directory.</summary>
		/// <remarks>
		/// Get an xattr name and value for a file or directory.
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

		/// <summary>Get all of the xattr name/value pairs for a file or directory.</summary>
		/// <remarks>
		/// Get all of the xattr name/value pairs for a file or directory.
		/// Only those xattrs which the logged-in user has permissions to view
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

		/// <summary>Get all of the xattrs name/value pairs for a file or directory.</summary>
		/// <remarks>
		/// Get all of the xattrs name/value pairs for a file or directory.
		/// Only those xattrs which the logged-in user has permissions to view
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
		/// Only those xattr names which the logged-in user has permissions to view
		/// are returned.
		/// <p/>
		/// Refer to the HDFS extended attributes user documentation for details.
		/// </remarks>
		/// <param name="path">Path to get extended attributes</param>
		/// <returns>List<String> of the XAttr names of the file or directory</returns>
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

		private static volatile bool FILE_SYSTEMS_LOADED = false;

		private static readonly System.Collections.Generic.IDictionary<string, java.lang.Class
			> SERVICE_FILE_SYSTEMS = new System.Collections.Generic.Dictionary<string, java.lang.Class
			>();

		// making it volatile to be able to do a double checked locking
		private static void loadFileSystems()
		{
			lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)))
			{
				if (!FILE_SYSTEMS_LOADED)
				{
					java.util.ServiceLoader<org.apache.hadoop.fs.FileSystem> serviceLoader = java.util.ServiceLoader
						.load<org.apache.hadoop.fs.FileSystem>();
					foreach (org.apache.hadoop.fs.FileSystem fs in serviceLoader)
					{
						SERVICE_FILE_SYSTEMS[fs.getScheme()] = Sharpen.Runtime.getClassForObject(fs);
					}
					FILE_SYSTEMS_LOADED = true;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static java.lang.Class getFileSystemClass(string scheme, org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (!FILE_SYSTEMS_LOADED)
			{
				loadFileSystems();
			}
			java.lang.Class clazz = null;
			if (conf != null)
			{
				clazz = (java.lang.Class)conf.getClass("fs." + scheme + ".impl", null);
			}
			if (clazz == null)
			{
				clazz = SERVICE_FILE_SYSTEMS[scheme];
			}
			if (clazz == null)
			{
				throw new System.IO.IOException("No FileSystem for scheme: " + scheme);
			}
			return clazz;
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.FileSystem createFileSystem(java.net.URI uri, 
			org.apache.hadoop.conf.Configuration conf)
		{
			java.lang.Class clazz = getFileSystemClass(uri.getScheme(), conf);
			org.apache.hadoop.fs.FileSystem fs = (org.apache.hadoop.fs.FileSystem)org.apache.hadoop.util.ReflectionUtils
				.newInstance(clazz, conf);
			fs.initialize(uri, conf);
			return fs;
		}

		/// <summary>Caching FileSystem objects</summary>
		internal class Cache
		{
			private readonly org.apache.hadoop.fs.FileSystem.Cache.ClientFinalizer clientFinalizer;

			private readonly System.Collections.Generic.IDictionary<org.apache.hadoop.fs.FileSystem.Cache.Key
				, org.apache.hadoop.fs.FileSystem> map = new System.Collections.Generic.Dictionary
				<org.apache.hadoop.fs.FileSystem.Cache.Key, org.apache.hadoop.fs.FileSystem>();

			private readonly System.Collections.Generic.ICollection<org.apache.hadoop.fs.FileSystem.Cache.Key
				> toAutoClose = new java.util.HashSet<org.apache.hadoop.fs.FileSystem.Cache.Key>
				();

			/// <summary>A variable that makes all objects in the cache unique</summary>
			private static java.util.concurrent.atomic.AtomicLong unique = new java.util.concurrent.atomic.AtomicLong
				(1);

			/// <exception cref="System.IO.IOException"/>
			internal virtual org.apache.hadoop.fs.FileSystem get(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				org.apache.hadoop.fs.FileSystem.Cache.Key key = new org.apache.hadoop.fs.FileSystem.Cache.Key
					(uri, conf);
				return getInternal(uri, conf, key);
			}

			/// <summary>The objects inserted into the cache using this method are all unique</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual org.apache.hadoop.fs.FileSystem getUnique(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				org.apache.hadoop.fs.FileSystem.Cache.Key key = new org.apache.hadoop.fs.FileSystem.Cache.Key
					(uri, conf, unique.getAndIncrement());
				return getInternal(uri, conf, key);
			}

			/// <exception cref="System.IO.IOException"/>
			private org.apache.hadoop.fs.FileSystem getInternal(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf, org.apache.hadoop.fs.FileSystem.Cache.Key key)
			{
				org.apache.hadoop.fs.FileSystem fs;
				lock (this)
				{
					fs = map[key];
				}
				if (fs != null)
				{
					return fs;
				}
				fs = createFileSystem(uri, conf);
				lock (this)
				{
					// refetch the lock again
					org.apache.hadoop.fs.FileSystem oldfs = map[key];
					if (oldfs != null)
					{
						// a file system is created while lock is releasing
						fs.close();
						// close the new file system
						return oldfs;
					}
					// return the old file system
					// now insert the new file system into the map
					if (map.isEmpty() && !org.apache.hadoop.util.ShutdownHookManager.get().isShutdownInProgress
						())
					{
						org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(clientFinalizer, 
							SHUTDOWN_HOOK_PRIORITY);
					}
					fs.key = key;
					map[key] = fs;
					if (conf.getBoolean("fs.automatic.close", true))
					{
						toAutoClose.add(key);
					}
					return fs;
				}
			}

			internal virtual void remove(org.apache.hadoop.fs.FileSystem.Cache.Key key, org.apache.hadoop.fs.FileSystem
				 fs)
			{
				lock (this)
				{
					if (map.Contains(key) && fs == map[key])
					{
						Sharpen.Collections.Remove(map, key);
						toAutoClose.remove(key);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void closeAll()
			{
				lock (this)
				{
					closeAll(false);
				}
			}

			/// <summary>Close all FileSystem instances in the Cache.</summary>
			/// <param name="onlyAutomatic">only close those that are marked for automatic closing
			/// 	</param>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void closeAll(bool onlyAutomatic)
			{
				lock (this)
				{
					System.Collections.Generic.IList<System.IO.IOException> exceptions = new System.Collections.Generic.List
						<System.IO.IOException>();
					// Make a copy of the keys in the map since we'll be modifying
					// the map while iterating over it, which isn't safe.
					System.Collections.Generic.IList<org.apache.hadoop.fs.FileSystem.Cache.Key> keys = 
						new System.Collections.Generic.List<org.apache.hadoop.fs.FileSystem.Cache.Key>();
					Sharpen.Collections.AddAll(keys, map.Keys);
					foreach (org.apache.hadoop.fs.FileSystem.Cache.Key key in keys)
					{
						org.apache.hadoop.fs.FileSystem fs = map[key];
						if (onlyAutomatic && !toAutoClose.contains(key))
						{
							continue;
						}
						//remove from cache
						remove(key, fs);
						if (fs != null)
						{
							try
							{
								fs.close();
							}
							catch (System.IO.IOException ioe)
							{
								exceptions.add(ioe);
							}
						}
					}
					if (!exceptions.isEmpty())
					{
						throw org.apache.hadoop.io.MultipleIOException.createIOException(exceptions);
					}
				}
			}

			private class ClientFinalizer : java.lang.Runnable
			{
				public virtual void run()
				{
					lock (this)
					{
						try
						{
							this._enclosing.closeAll(true);
						}
						catch (System.IO.IOException e)
						{
							org.apache.hadoop.fs.FileSystem.LOG.info("FileSystem.Cache.closeAll() threw an exception:\n"
								 + e);
						}
					}
				}

				internal ClientFinalizer(Cache _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly Cache _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void closeAll(org.apache.hadoop.security.UserGroupInformation ugi
				)
			{
				lock (this)
				{
					System.Collections.Generic.IList<org.apache.hadoop.fs.FileSystem> targetFSList = 
						new System.Collections.Generic.List<org.apache.hadoop.fs.FileSystem>();
					//Make a pass over the list and collect the filesystems to close
					//we cannot close inline since close() removes the entry from the Map
					foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.fs.FileSystem.Cache.Key
						, org.apache.hadoop.fs.FileSystem> entry in map)
					{
						org.apache.hadoop.fs.FileSystem.Cache.Key key = entry.Key;
						org.apache.hadoop.fs.FileSystem fs = entry.Value;
						if (ugi.Equals(key.ugi) && fs != null)
						{
							targetFSList.add(fs);
						}
					}
					System.Collections.Generic.IList<System.IO.IOException> exceptions = new System.Collections.Generic.List
						<System.IO.IOException>();
					//now make a pass over the target list and close each
					foreach (org.apache.hadoop.fs.FileSystem fs_1 in targetFSList)
					{
						try
						{
							fs_1.close();
						}
						catch (System.IO.IOException ioe)
						{
							exceptions.add(ioe);
						}
					}
					if (!exceptions.isEmpty())
					{
						throw org.apache.hadoop.io.MultipleIOException.createIOException(exceptions);
					}
				}
			}

			/// <summary>FileSystem.Cache.Key</summary>
			internal class Key
			{
				internal readonly string scheme;

				internal readonly string authority;

				internal readonly org.apache.hadoop.security.UserGroupInformation ugi;

				internal readonly long unique;

				/// <exception cref="System.IO.IOException"/>
				internal Key(java.net.URI uri, org.apache.hadoop.conf.Configuration conf)
					: this(uri, conf, 0)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				internal Key(java.net.URI uri, org.apache.hadoop.conf.Configuration conf, long unique
					)
				{
					// an artificial way to make a key unique
					scheme = uri.getScheme() == null ? string.Empty : org.apache.hadoop.util.StringUtils
						.toLowerCase(uri.getScheme());
					authority = uri.getAuthority() == null ? string.Empty : org.apache.hadoop.util.StringUtils
						.toLowerCase(uri.getAuthority());
					this.unique = unique;
					this.ugi = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
				}

				public override int GetHashCode()
				{
					return (scheme + authority).GetHashCode() + ugi.GetHashCode() + (int)unique;
				}

				internal static bool isEqual(object a, object b)
				{
					return a == b || (a != null && a.Equals(b));
				}

				public override bool Equals(object obj)
				{
					if (obj == this)
					{
						return true;
					}
					if (obj != null && obj is org.apache.hadoop.fs.FileSystem.Cache.Key)
					{
						org.apache.hadoop.fs.FileSystem.Cache.Key that = (org.apache.hadoop.fs.FileSystem.Cache.Key
							)obj;
						return isEqual(this.scheme, that.scheme) && isEqual(this.authority, that.authority
							) && isEqual(this.ugi, that.ugi) && (this.unique == that.unique);
					}
					return false;
				}

				public override string ToString()
				{
					return "(" + ugi.ToString() + ")@" + scheme + "://" + authority;
				}
			}

			public Cache()
			{
				clientFinalizer = new org.apache.hadoop.fs.FileSystem.Cache.ClientFinalizer(this);
			}
		}

		/// <summary>
		/// Tracks statistics about how many reads, writes, and so forth have been
		/// done in a FileSystem.
		/// </summary>
		/// <remarks>
		/// Tracks statistics about how many reads, writes, and so forth have been
		/// done in a FileSystem.
		/// Since there is only one of these objects per FileSystem, there will
		/// typically be many threads writing to this object.  Almost every operation
		/// on an open file will involve a write to this object.  In contrast, reading
		/// statistics is done infrequently by most programs, and not at all by others.
		/// Hence, this is optimized for writes.
		/// Each thread writes to its own thread-local area of memory.  This removes
		/// contention and allows us to scale up to many, many threads.  To read
		/// statistics, the reader thread totals up the contents of all of the
		/// thread-local data areas.
		/// </remarks>
		public sealed class Statistics
		{
			/// <summary>Statistics data.</summary>
			/// <remarks>
			/// Statistics data.
			/// There is only a single writer to thread-local StatisticsData objects.
			/// Hence, volatile is adequate here-- we do not need AtomicLong or similar
			/// to prevent lost updates.
			/// The Java specification guarantees that updates to volatile longs will
			/// be perceived as atomic with respect to other threads, which is all we
			/// need.
			/// </remarks>
			public class StatisticsData
			{
				internal volatile long bytesRead;

				internal volatile long bytesWritten;

				internal volatile int readOps;

				internal volatile int largeReadOps;

				internal volatile int writeOps;

				/// <summary>Stores a weak reference to the thread owning this StatisticsData.</summary>
				/// <remarks>
				/// Stores a weak reference to the thread owning this StatisticsData.
				/// This allows us to remove StatisticsData objects that pertain to
				/// threads that no longer exist.
				/// </remarks>
				internal readonly java.lang.@ref.WeakReference<java.lang.Thread> owner;

				internal StatisticsData(java.lang.@ref.WeakReference<java.lang.Thread> owner)
				{
					this.owner = owner;
				}

				/// <summary>Add another StatisticsData object to this one.</summary>
				internal virtual void add(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
					 other)
				{
					this.bytesRead += other.bytesRead;
					this.bytesWritten += other.bytesWritten;
					this.readOps += other.readOps;
					this.largeReadOps += other.largeReadOps;
					this.writeOps += other.writeOps;
				}

				/// <summary>Negate the values of all statistics.</summary>
				internal virtual void negate()
				{
					this.bytesRead = -this.bytesRead;
					this.bytesWritten = -this.bytesWritten;
					this.readOps = -this.readOps;
					this.largeReadOps = -this.largeReadOps;
					this.writeOps = -this.writeOps;
				}

				public override string ToString()
				{
					return bytesRead + " bytes read, " + bytesWritten + " bytes written, " + readOps 
						+ " read ops, " + largeReadOps + " large read ops, " + writeOps + " write ops";
				}

				public virtual long getBytesRead()
				{
					return bytesRead;
				}

				public virtual long getBytesWritten()
				{
					return bytesWritten;
				}

				public virtual int getReadOps()
				{
					return readOps;
				}

				public virtual int getLargeReadOps()
				{
					return largeReadOps;
				}

				public virtual int getWriteOps()
				{
					return writeOps;
				}
			}

			private interface StatisticsAggregator<T>
			{
				void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data);

				T aggregate();
			}

			private readonly string scheme;

			/// <summary>
			/// rootData is data that doesn't belong to any thread, but will be added
			/// to the totals.
			/// </summary>
			/// <remarks>
			/// rootData is data that doesn't belong to any thread, but will be added
			/// to the totals.  This is useful for making copies of Statistics objects,
			/// and for storing data that pertains to threads that have been garbage
			/// collected.  Protected by the Statistics lock.
			/// </remarks>
			private readonly org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData rootData;

			/// <summary>Thread-local data.</summary>
			private readonly java.lang.ThreadLocal<org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
				> threadData;

			/// <summary>List of all thread-local data areas.</summary>
			/// <remarks>List of all thread-local data areas.  Protected by the Statistics lock.</remarks>
			private System.Collections.Generic.LinkedList<org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
				> allData;

			public Statistics(string scheme)
			{
				this.scheme = scheme;
				this.rootData = new org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData(null
					);
				this.threadData = new java.lang.ThreadLocal<org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
					>();
				this.allData = null;
			}

			/// <summary>Copy constructor.</summary>
			/// <param name="other">The input Statistics object which is cloned.</param>
			public Statistics(org.apache.hadoop.fs.FileSystem.Statistics other)
			{
				this.scheme = other.scheme;
				this.rootData = new org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData(null
					);
				other.visitAll(new _StatisticsAggregator_2979(this));
				this.threadData = new java.lang.ThreadLocal<org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
					>();
			}

			private sealed class _StatisticsAggregator_2979 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<java.lang.Void>
			{
				public _StatisticsAggregator_2979(Statistics _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this._enclosing.rootData.add(data);
				}

				public java.lang.Void aggregate()
				{
					return null;
				}

				private readonly Statistics _enclosing;
			}

			/// <summary>Get or create the thread-local data associated with the current thread.</summary>
			public org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData getThreadStatistics
				()
			{
				org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data = threadData.get();
				if (data == null)
				{
					data = new org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData(new java.lang.@ref.WeakReference
						<java.lang.Thread>(java.lang.Thread.currentThread()));
					threadData.set(data);
					lock (this)
					{
						if (allData == null)
						{
							allData = new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
								>();
						}
						allData.add(data);
					}
				}
				return data;
			}

			/// <summary>Increment the bytes read in the statistics</summary>
			/// <param name="newBytes">the additional bytes read</param>
			public void incrementBytesRead(long newBytes)
			{
				getThreadStatistics().bytesRead += newBytes;
			}

			/// <summary>Increment the bytes written in the statistics</summary>
			/// <param name="newBytes">the additional bytes written</param>
			public void incrementBytesWritten(long newBytes)
			{
				getThreadStatistics().bytesWritten += newBytes;
			}

			/// <summary>Increment the number of read operations</summary>
			/// <param name="count">number of read operations</param>
			public void incrementReadOps(int count)
			{
				getThreadStatistics().readOps += count;
			}

			/// <summary>Increment the number of large read operations</summary>
			/// <param name="count">number of large read operations</param>
			public void incrementLargeReadOps(int count)
			{
				getThreadStatistics().largeReadOps += count;
			}

			/// <summary>Increment the number of write operations</summary>
			/// <param name="count">number of write operations</param>
			public void incrementWriteOps(int count)
			{
				getThreadStatistics().writeOps += count;
			}

			/// <summary>
			/// Apply the given aggregator to all StatisticsData objects associated with
			/// this Statistics object.
			/// </summary>
			/// <remarks>
			/// Apply the given aggregator to all StatisticsData objects associated with
			/// this Statistics object.
			/// For each StatisticsData object, we will call accept on the visitor.
			/// Finally, at the end, we will call aggregate to get the final total.
			/// </remarks>
			/// <param name="The">visitor to use.</param>
			/// <returns>The total.</returns>
			private T visitAll<T>(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<T> visitor)
			{
				lock (this)
				{
					visitor.accept(rootData);
					if (allData != null)
					{
						for (System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData
							> iter = allData.GetEnumerator(); iter.MoveNext(); )
						{
							org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data = iter.Current;
							visitor.accept(data);
							if (data.owner.get() == null)
							{
								/*
								* If the thread that created this thread-local data no
								* longer exists, remove the StatisticsData from our list
								* and fold the values into rootData.
								*/
								rootData.add(data);
								iter.remove();
							}
						}
					}
					return visitor.aggregate();
				}
			}

			/// <summary>Get the total number of bytes read</summary>
			/// <returns>the number of bytes</returns>
			public long getBytesRead()
			{
				return visitAll(new _StatisticsAggregator_3087());
			}

			private sealed class _StatisticsAggregator_3087 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<long>
			{
				public _StatisticsAggregator_3087()
				{
					this.bytesRead = 0;
				}

				private long bytesRead;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.bytesRead += data.bytesRead;
				}

				public long aggregate()
				{
					return this.bytesRead;
				}
			}

			/// <summary>Get the total number of bytes written</summary>
			/// <returns>the number of bytes</returns>
			public long getBytesWritten()
			{
				return visitAll(new _StatisticsAggregator_3106());
			}

			private sealed class _StatisticsAggregator_3106 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<long>
			{
				public _StatisticsAggregator_3106()
				{
					this.bytesWritten = 0;
				}

				private long bytesWritten;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.bytesWritten += data.bytesWritten;
				}

				public long aggregate()
				{
					return this.bytesWritten;
				}
			}

			/// <summary>Get the number of file system read operations such as list files</summary>
			/// <returns>number of read operations</returns>
			public int getReadOps()
			{
				return visitAll(new _StatisticsAggregator_3125());
			}

			private sealed class _StatisticsAggregator_3125 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<int>
			{
				public _StatisticsAggregator_3125()
				{
					this.readOps = 0;
				}

				private int readOps;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.readOps += data.readOps;
					this.readOps += data.largeReadOps;
				}

				public int aggregate()
				{
					return this.readOps;
				}
			}

			/// <summary>
			/// Get the number of large file system read operations such as list files
			/// under a large directory
			/// </summary>
			/// <returns>number of large read operations</returns>
			public int getLargeReadOps()
			{
				return visitAll(new _StatisticsAggregator_3146());
			}

			private sealed class _StatisticsAggregator_3146 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<int>
			{
				public _StatisticsAggregator_3146()
				{
					this.largeReadOps = 0;
				}

				private int largeReadOps;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.largeReadOps += data.largeReadOps;
				}

				public int aggregate()
				{
					return this.largeReadOps;
				}
			}

			/// <summary>
			/// Get the number of file system write operations such as create, append
			/// rename etc.
			/// </summary>
			/// <returns>number of write operations</returns>
			public int getWriteOps()
			{
				return visitAll(new _StatisticsAggregator_3166());
			}

			private sealed class _StatisticsAggregator_3166 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<int>
			{
				public _StatisticsAggregator_3166()
				{
					this.writeOps = 0;
				}

				private int writeOps;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.writeOps += data.writeOps;
				}

				public int aggregate()
				{
					return this.writeOps;
				}
			}

			public override string ToString()
			{
				return visitAll(new _StatisticsAggregator_3183());
			}

			private sealed class _StatisticsAggregator_3183 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<string>
			{
				public _StatisticsAggregator_3183()
				{
					this.total = new org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData(null);
				}

				private org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData total;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.total.add(data);
				}

				public string aggregate()
				{
					return this.total.ToString();
				}
			}

			/// <summary>Resets all statistics to 0.</summary>
			/// <remarks>
			/// Resets all statistics to 0.
			/// In order to reset, we add up all the thread-local statistics data, and
			/// set rootData to the negative of that.
			/// This may seem like a counterintuitive way to reset the statsitics.  Why
			/// can't we just zero out all the thread-local data?  Well, thread-local
			/// data can only be modified by the thread that owns it.  If we tried to
			/// modify the thread-local data from this thread, our modification might get
			/// interleaved with a read-modify-write operation done by the thread that
			/// owns the data.  That would result in our update getting lost.
			/// The approach used here avoids this problem because it only ever reads
			/// (not writes) the thread-local data.  Both reads and writes to rootData
			/// are done under the lock, so we're free to modify rootData from any thread
			/// that holds the lock.
			/// </remarks>
			public void reset()
			{
				visitAll(new _StatisticsAggregator_3216(this));
			}

			private sealed class _StatisticsAggregator_3216 : org.apache.hadoop.fs.FileSystem.Statistics.StatisticsAggregator
				<java.lang.Void>
			{
				public _StatisticsAggregator_3216(Statistics _enclosing)
				{
					this._enclosing = _enclosing;
					this.total = new org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData(null);
				}

				private org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData total;

				public void accept(org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData data
					)
				{
					this.total.add(data);
				}

				public java.lang.Void aggregate()
				{
					this.total.negate();
					this._enclosing.rootData.add(this.total);
					return null;
				}

				private readonly Statistics _enclosing;
			}

			/// <summary>Get the uri scheme associated with this statistics object.</summary>
			/// <returns>the schema associated with this set of statistics</returns>
			public string getScheme()
			{
				return scheme;
			}
		}

		/// <summary>Get the Map of Statistics object indexed by URI Scheme.</summary>
		/// <returns>a Map having a key as URI scheme and value as Statistics object</returns>
		[System.ObsoleteAttribute(@"use getAllStatistics() instead")]
		public static System.Collections.Generic.IDictionary<string, org.apache.hadoop.fs.FileSystem.Statistics
			> getStatistics()
		{
			lock (typeof(FileSystem))
			{
				System.Collections.Generic.IDictionary<string, org.apache.hadoop.fs.FileSystem.Statistics
					> result = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.fs.FileSystem.Statistics
					>();
				foreach (org.apache.hadoop.fs.FileSystem.Statistics stat in statisticsTable.Values)
				{
					result[stat.getScheme()] = stat;
				}
				return result;
			}
		}

		/// <summary>Return the FileSystem classes that have Statistics</summary>
		public static System.Collections.Generic.IList<org.apache.hadoop.fs.FileSystem.Statistics
			> getAllStatistics()
		{
			lock (typeof(FileSystem))
			{
				return new System.Collections.Generic.List<org.apache.hadoop.fs.FileSystem.Statistics
					>(statisticsTable.Values);
			}
		}

		/// <summary>Get the statistics for a particular file system</summary>
		/// <param name="cls">the class to lookup</param>
		/// <returns>a statistics object</returns>
		public static org.apache.hadoop.fs.FileSystem.Statistics getStatistics(string scheme
			, java.lang.Class cls)
		{
			lock (typeof(FileSystem))
			{
				org.apache.hadoop.fs.FileSystem.Statistics result = statisticsTable[cls];
				if (result == null)
				{
					result = new org.apache.hadoop.fs.FileSystem.Statistics(scheme);
					statisticsTable[cls] = result;
				}
				return result;
			}
		}

		/// <summary>Reset all statistics for all file systems</summary>
		public static void clearStatistics()
		{
			lock (typeof(FileSystem))
			{
				foreach (org.apache.hadoop.fs.FileSystem.Statistics stat in statisticsTable.Values)
				{
					stat.reset();
				}
			}
		}

		/// <summary>Print all statistics for all file systems</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void printStatistics()
		{
			lock (typeof(FileSystem))
			{
				foreach (System.Collections.Generic.KeyValuePair<java.lang.Class, org.apache.hadoop.fs.FileSystem.Statistics
					> pair in statisticsTable)
				{
					System.Console.Out.WriteLine("  FileSystem " + pair.Key.getName() + ": " + pair.Value
						);
				}
			}
		}

		private static bool symlinksEnabled = false;

		private static org.apache.hadoop.conf.Configuration conf = null;

		// Symlinks are temporarily disabled - see HADOOP-10020 and HADOOP-10052
		[com.google.common.annotations.VisibleForTesting]
		public static bool areSymlinksEnabled()
		{
			return symlinksEnabled;
		}

		[com.google.common.annotations.VisibleForTesting]
		public static void enableSymlinks()
		{
			symlinksEnabled = true;
		}
	}
}

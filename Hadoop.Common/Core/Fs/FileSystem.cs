using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Options = Org.Apache.Hadoop.FS.Options;
using Path = Org.Apache.Hadoop.FS.Path;

namespace Hadoop.Common.Core.Fs
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
	public abstract class FileSystem : Configured, IDisposable
	{
		public const string FsDefaultNameKey = CommonConfigurationKeys.FsDefaultNameKey;

		public const string DefaultFs = CommonConfigurationKeys.FsDefaultNameDefault;

		public static readonly Org.Apache.Hadoop.Log Log = LogFactory.GetLog(typeof(FileSystem));

		/// <summary>Priority of the FileSystem shutdown hook.</summary>
		public const int ShutdownHookPriority = 10;

		/// <summary>FileSystem cache</summary>
		internal static readonly FileSystem.Cache Cache = new FileSystem.Cache();

		/// <summary>The key this instance is stored under in the cache.</summary>
		private FileSystem.Cache.Key key;

		/// <summary>Recording statistics per a FileSystem class</summary>
		private static readonly IDictionary<Type, FileSystem.Statistics> statisticsTable = 
			new IdentityHashMap<Type, FileSystem.Statistics>();

		/// <summary>The statistics for this file system.</summary>
		protected internal FileSystem.Statistics statistics;

		/// <summary>
		/// A cache of files that should be deleted when filsystem is closed
		/// or the JVM is exited.
		/// </summary>
		private ICollection<Path> deleteOnExit = new TreeSet<Path>();

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
		internal static void AddFileSystemForTesting(Uri uri, Configuration conf, FileSystem fs)
		{
			Cache.map[new FileSystem.Cache.Key(uri, conf)] = fs;
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
		public static FileSystem Get(URI uri, Configuration conf, string
			 user)
		{
			string ticketCachePath = conf.Get(CommonConfigurationKeys.KerberosTicketCachePath
				);
			UserGroupInformation ugi = UserGroupInformation.GetBestUGI(ticketCachePath, user);
			return ugi.DoAs(new _PrivilegedExceptionAction_157(uri, conf));
		}

		private sealed class _PrivilegedExceptionAction_157 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_157(URI uri, Configuration conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileSystem Run()
			{
				return FileSystem.Get(uri, conf);
			}

			private readonly URI uri;

			private readonly Configuration conf;
		}

		/// <summary>Returns the configured filesystem implementation.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <exception cref="System.IO.IOException"/>
		public static FileSystem Get(Configuration conf)
		{
			return Get(GetDefaultUri(conf), conf);
		}

		/// <summary>Get the default filesystem URI from a configuration.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <returns>the uri of the default filesystem</returns>
		public static URI GetDefaultUri(Configuration conf)
		{
			return URI.Create(FixName(conf.Get(FsDefaultNameKey, DefaultFs)));
		}

		/// <summary>Set the default filesystem URI in a configuration.</summary>
		/// <param name="conf">the configuration to alter</param>
		/// <param name="uri">the new default filesystem uri</param>
		public static void SetDefaultUri(Configuration conf, URI uri)
		{
			conf.Set(FsDefaultNameKey, uri.ToString());
		}

		/// <summary>Set the default filesystem URI in a configuration.</summary>
		/// <param name="conf">the configuration to alter</param>
		/// <param name="uri">the new default filesystem uri</param>
		public static void SetDefaultUri(Configuration conf, string uri)
		{
			SetDefaultUri(conf, URI.Create(FixName(uri)));
		}

		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="name">
		/// a uri whose authority section names the host, port, etc.
		/// for this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Initialize(Uri name, Configuration conf)
		{
			statistics = GetStatistics(name.Scheme, GetType());
			resolveSymlinks = conf.GetBoolean(CommonConfigurationKeys.FsClientResolveRemoteSymlinksKey
				, CommonConfigurationKeys.FsClientResolveRemoteSymlinksDefault);
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// This implementation throws an <code>UnsupportedOperationException</code>.
		/// </remarks>
		/// <returns>the protocol scheme for the FileSystem.</returns>
		public virtual string GetScheme()
		{
			throw new NotSupportedException("Not implemented by the " + GetType().Name + " FileSystem implementation"
				);
		}

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		public abstract URI GetUri();

		/// <summary>Return a canonicalized form of this FileSystem's URI.</summary>
		/// <remarks>
		/// Return a canonicalized form of this FileSystem's URI.
		/// The default implementation simply calls
		/// <see cref="CanonicalizeUri(Sharpen.URI)"/>
		/// on the filesystem's own URI, so subclasses typically only need to
		/// implement that method.
		/// </remarks>
		/// <seealso cref="CanonicalizeUri(Sharpen.URI)"/>
		protected internal virtual URI GetCanonicalUri()
		{
			return CanonicalizeUri(GetUri());
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
		/// <seealso cref="Org.Apache.Hadoop.Net.NetUtils.GetCanonicalUri(Sharpen.URI, int)"/
		/// 	>
		protected internal virtual URI CanonicalizeUri(URI uri)
		{
			if (uri.GetPort() == -1 && GetDefaultPort() > 0)
			{
				// reconstruct the uri with the default port set
				try
				{
					uri = new URI(uri.GetScheme(), uri.GetUserInfo(), uri.GetHost(), GetDefaultPort()
						, uri.GetPath(), uri.GetQuery(), uri.GetFragment());
				}
				catch (URISyntaxException)
				{
					// Should never happen!
					throw new Exception("Valid URI became unparseable: " + uri);
				}
			}
			return uri;
		}

		/// <summary>Get the default port for this file system.</summary>
		/// <returns>the default port or 0 if there isn't one</returns>
		protected internal virtual int GetDefaultPort()
		{
			return 0;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal static FileSystem GetFSofPath(Path absOrFqPath
			, Configuration conf)
		{
			absOrFqPath.CheckNotSchemeWithRelative();
			absOrFqPath.CheckNotRelative();
			// Uses the default file system if not fully qualified
			return Get(absOrFqPath.ToUri(), conf);
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
		/// <seealso cref="Org.Apache.Hadoop.Security.SecurityUtil.BuildDTServiceName(Sharpen.URI, int)
		/// 	"></seealso>
		public virtual string GetCanonicalServiceName()
		{
			return (GetChildFileSystems() == null) ? SecurityUtil.BuildDTServiceName(GetUri()
				, GetDefaultPort()) : null;
		}

		[Obsolete(@"call #getUri() instead.")]
		public virtual string GetName()
		{
			return GetUri().ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete(@"call #get(URI,Configuration) instead.")]
		public static FileSystem GetNamed(string name, Configuration
			 conf)
		{
			return Get(URI.Create(FixName(name)), conf);
		}

		/// <summary>Update old-format filesystem names, for back-compatibility.</summary>
		/// <remarks>
		/// Update old-format filesystem names, for back-compatibility.  This should
		/// eventually be replaced with a checkName() method that throws an exception
		/// for old-format names.
		/// </remarks>
		private static string FixName(string name)
		{
			// convert old-format name to new-format name
			if (name.Equals("local"))
			{
				// "local" is now "file:///".
				Log.Warn("\"local\" is a deprecated filesystem name." + " Use \"file:///\" instead."
					);
				name = "file:///";
			}
			else
			{
				if (name.IndexOf('/') == -1)
				{
					// unqualified is "hdfs://"
					Log.Warn("\"" + name + "\" is a deprecated filesystem name." + " Use \"hdfs://" +
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
		public static LocalFileSystem GetLocal(Configuration conf)
		{
			return (LocalFileSystem)Get(LocalFileSystem.Name, conf);
		}

		/// <summary>Returns the FileSystem for this URI's scheme and authority.</summary>
		/// <remarks>
		/// Returns the FileSystem for this URI's scheme and authority.  The scheme
		/// of the URI determines a configuration property name,
		/// <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
		/// The entire URI is passed to the FileSystem instance's initialize method.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static FileSystem Get(URI uri, Configuration conf)
		{
			string scheme = uri.GetScheme();
			string authority = uri.GetAuthority();
			if (scheme == null && authority == null)
			{
				// use default FS
				return Get(conf);
			}
			if (scheme != null && authority == null)
			{
				// no authority
				URI defaultUri = GetDefaultUri(conf);
				if (scheme.Equals(defaultUri.GetScheme()) && defaultUri.GetAuthority() != null)
				{
					// if scheme matches default
					// & default has authority
					return Get(defaultUri, conf);
				}
			}
			// return default
			string disableCacheName = string.Format("fs.%s.impl.disable.cache", scheme);
			if (conf.GetBoolean(disableCacheName, false))
			{
				return CreateFileSystem(uri, conf);
			}
			return Cache.Get(uri, conf);
		}

		/// <summary>
		/// Returns the FileSystem for this URI's scheme and authority and the
		/// passed user.
		/// </summary>
		/// <remarks>
		/// Returns the FileSystem for this URI's scheme and authority and the
		/// passed user. Internally invokes
		/// <see cref="NewInstance(Sharpen.URI, Org.Apache.Hadoop.Conf.Configuration)"/>
		/// </remarks>
		/// <param name="uri">of the filesystem</param>
		/// <param name="conf">the configuration to use</param>
		/// <param name="user">to perform the get as</param>
		/// <returns>filesystem instance</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static FileSystem NewInstance(URI uri, Configuration 
			conf, string user)
		{
			string ticketCachePath = conf.Get(CommonConfigurationKeys.KerberosTicketCachePath
				);
			UserGroupInformation ugi = UserGroupInformation.GetBestUGI(ticketCachePath, user);
			return ugi.DoAs(new _PrivilegedExceptionAction_390(uri, conf));
		}

		private sealed class _PrivilegedExceptionAction_390 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_390(URI uri, Configuration conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileSystem Run()
			{
				return FileSystem.NewInstance(uri, conf);
			}

			private readonly URI uri;

			private readonly Configuration conf;
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
		public static FileSystem NewInstance(URI uri, Configuration 
			conf)
		{
			string scheme = uri.GetScheme();
			string authority = uri.GetAuthority();
			if (scheme == null)
			{
				// no scheme: use default FS
				return NewInstance(conf);
			}
			if (authority == null)
			{
				// no authority
				URI defaultUri = GetDefaultUri(conf);
				if (scheme.Equals(defaultUri.GetScheme()) && defaultUri.GetAuthority() != null)
				{
					// if scheme matches default
					// & default has authority
					return NewInstance(defaultUri, conf);
				}
			}
			// return default
			return Cache.GetUnique(uri, conf);
		}

		/// <summary>Returns a unique configured filesystem implementation.</summary>
		/// <remarks>
		/// Returns a unique configured filesystem implementation.
		/// This always returns a new FileSystem object.
		/// </remarks>
		/// <param name="conf">the configuration to use</param>
		/// <exception cref="System.IO.IOException"/>
		public static FileSystem NewInstance(Configuration conf)
		{
			return NewInstance(GetDefaultUri(conf), conf);
		}

		/// <summary>Get a unique local file system object</summary>
		/// <param name="conf">the configuration to configure the file system with</param>
		/// <returns>
		/// a LocalFileSystem
		/// This always returns a new FileSystem object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static LocalFileSystem NewInstanceLocal(Configuration conf)
		{
			return (LocalFileSystem)NewInstance(LocalFileSystem.Name, conf);
		}

		/// <summary>Close all cached filesystems.</summary>
		/// <remarks>
		/// Close all cached filesystems. Be sure those filesystems are not
		/// used anymore.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void CloseAll()
		{
			Cache.CloseAll();
		}

		/// <summary>Close all cached filesystems for a given UGI.</summary>
		/// <remarks>
		/// Close all cached filesystems for a given UGI. Be sure those filesystems
		/// are not used anymore.
		/// </remarks>
		/// <param name="ugi">user group info to close</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CloseAllForUGI(UserGroupInformation ugi)
		{
			Cache.CloseAll(ugi);
		}

		/// <summary>Make sure that a path specifies a FileSystem.</summary>
		/// <param name="path">to use</param>
		public virtual Path MakeQualified(Path path)
		{
			CheckPath(path);
			return path.MakeQualified(this.GetUri(), this.GetWorkingDirectory());
		}

		/// <summary>Get a new delegation token for this file system.</summary>
		/// <remarks>
		/// Get a new delegation token for this file system.
		/// This is an internal method that should have been declared protected
		/// but wasn't historically.
		/// Callers should use
		/// <see cref="AddDelegationTokens(string, Org.Apache.Hadoop.Security.Credentials)"/>
		/// </remarks>
		/// <param name="renewer">the account name that is allowed to renew the token.</param>
		/// <returns>a new delegation token</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken(
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
		public virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
			(string renewer, Credentials credentials)
		{
			if (credentials == null)
			{
				credentials = new Credentials();
			}
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> tokens = new AList<Org.Apache.Hadoop.Security.Token.Token
				<object>>();
			CollectDelegationTokens(renewer, credentials, tokens);
			return Sharpen.Collections.ToArray(tokens, new Org.Apache.Hadoop.Security.Token.Token
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
		private void CollectDelegationTokens(string renewer, Credentials credentials, IList
			<Org.Apache.Hadoop.Security.Token.Token<object>> tokens)
		{
			string serviceName = GetCanonicalServiceName();
			// Collect token of the this filesystem and then of its embedded children
			if (serviceName != null)
			{
				// fs has token, grab it
				Text service = new Text(serviceName);
				Org.Apache.Hadoop.Security.Token.Token<object> token = credentials.GetToken(service
					);
				if (token == null)
				{
					token = GetDelegationToken(renewer);
					if (token != null)
					{
						tokens.AddItem(token);
						credentials.AddToken(service, token);
					}
				}
			}
			// Now collect the tokens from the children
			FileSystem[] children = GetChildFileSystems();
			if (children != null)
			{
				foreach (FileSystem fs in children)
				{
					fs.CollectDelegationTokens(renewer, credentials, tokens);
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
		[VisibleForTesting]
		public virtual FileSystem[] GetChildFileSystems()
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
		public static FSDataOutputStream Create(FileSystem fs, Path 
			file, FsPermission permission)
		{
			// create the file with default permission
			FSDataOutputStream @out = fs.Create(file);
			// set its permission to the supplied one
			fs.SetPermission(file, permission);
			return @out;
		}

		/// <summary>
		/// create a directory with the provided permission
		/// The permission of the directory is set to be the provided permission as in
		/// setPermission, not permission&~umask
		/// </summary>
		/// <seealso cref="Create(FileSystem, System.IO.Path, Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		/// <param name="fs">file system handle</param>
		/// <param name="dir">the name of the directory to be created</param>
		/// <param name="permission">the permission of the directory</param>
		/// <returns>true if the directory creation succeeds; false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool Mkdirs(FileSystem fs, Path dir, FsPermission
			 permission)
		{
			// create the directory using the default permission
			bool result = fs.Mkdirs(dir);
			// set its permission to be the supplied one
			fs.SetPermission(dir, permission);
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
		protected internal virtual void CheckPath(Path path)
		{
			URI uri = path.ToUri();
			string thatScheme = uri.GetScheme();
			if (thatScheme == null)
			{
				// fs is relative
				return;
			}
			URI thisUri = GetCanonicalUri();
			string thisScheme = thisUri.GetScheme();
			//authority and scheme are not case sensitive
			if (Sharpen.Runtime.EqualsIgnoreCase(thisScheme, thatScheme))
			{
				// schemes match
				string thisAuthority = thisUri.GetAuthority();
				string thatAuthority = uri.GetAuthority();
				if (thatAuthority == null && thisAuthority != null)
				{
					// path's authority is null
					// fs has an authority
					URI defaultUri = GetDefaultUri(GetConf());
					if (Sharpen.Runtime.EqualsIgnoreCase(thisScheme, defaultUri.GetScheme()))
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
					uri = CanonicalizeUri(uri);
					thatAuthority = uri.GetAuthority();
					if (thisAuthority == thatAuthority || (thisAuthority != null && Sharpen.Runtime.EqualsIgnoreCase
						(thisAuthority, thatAuthority)))
					{
						// authorities match
						return;
					}
				}
			}
			throw new ArgumentException("Wrong FS: " + path + ", expected: " + this.GetUri());
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
		public virtual BlockLocation[] GetFileBlockLocations(FileStatus file, long start, 
			long len)
		{
			if (file == null)
			{
				return null;
			}
			if (start < 0 || len < 0)
			{
				throw new ArgumentException("Invalid start or len parameter");
			}
			if (file.GetLen() <= start)
			{
				return new BlockLocation[0];
			}
			string[] name = new string[] { "localhost:50010" };
			string[] host = new string[] { "localhost" };
			return new BlockLocation[] { new BlockLocation(name, host, 0, file.GetLen()) };
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
		public virtual BlockLocation[] GetFileBlockLocations(Path p, long start, long len
			)
		{
			if (p == null)
			{
				throw new ArgumentNullException();
			}
			FileStatus file = GetFileStatus(p);
			return GetFileBlockLocations(file, start, len);
		}

		/// <summary>Return a set of server default configuration values</summary>
		/// <returns>server default configuration values</returns>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete(@"use GetServerDefaults(Path) instead")]
		public virtual FsServerDefaults GetServerDefaults()
		{
			Configuration conf = GetConf();
			// CRC32 is chosen as default as it is available in all 
			// releases that support checksum.
			// The client trash configuration is ignored.
			return new FsServerDefaults(GetDefaultBlockSize(), conf.GetInt("io.bytes.per.checksum"
				, 512), 64 * 1024, GetDefaultReplication(), conf.GetInt("io.file.buffer.size", 4096
				), false, CommonConfigurationKeysPublic.FsTrashIntervalDefault, DataChecksum.Type
				.Crc32);
		}

		/// <summary>Return a set of server default configuration values</summary>
		/// <param name="p">
		/// path is used to identify an FS since an FS could have
		/// another FS that it could be delegating the call to
		/// </param>
		/// <returns>server default configuration values</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FsServerDefaults GetServerDefaults(Path p)
		{
			return GetServerDefaults();
		}

		/// <summary>
		/// Return the fully-qualified path of path f resolving the path
		/// through any symlinks or mount point
		/// </summary>
		/// <param name="p">path to be resolved</param>
		/// <returns>fully qualified path</returns>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path ResolvePath(Path p)
		{
			CheckPath(p);
			return GetFileStatus(p).GetPath();
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract FSDataInputStream Open(Path f, int bufferSize);

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file to open</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataInputStream Open(Path f)
		{
			return Open(f, GetConf().GetInt("io.file.buffer.size", 4096));
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <remarks>
		/// Create an FSDataOutputStream at the indicated Path.
		/// Files are overwritten by default.
		/// </remarks>
		/// <param name="f">the file to create</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Create(Path f)
		{
			return Create(f, true);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <param name="f">the file to create</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an exception will be thrown.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Create(Path f, bool overwrite)
		{
			return Create(f, overwrite, GetConf().GetInt("io.file.buffer.size", 4096), GetDefaultReplication
				(f), GetDefaultBlockSize(f));
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
		public virtual FSDataOutputStream Create(Path f, Progressable progress)
		{
			return Create(f, true, GetConf().GetInt("io.file.buffer.size", 4096), GetDefaultReplication
				(f), GetDefaultBlockSize(f), progress);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <remarks>
		/// Create an FSDataOutputStream at the indicated Path.
		/// Files are overwritten by default.
		/// </remarks>
		/// <param name="f">the file to create</param>
		/// <param name="replication">the replication factor</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Create(Path f, short replication)
		{
			return Create(f, true, GetConf().GetInt("io.file.buffer.size", 4096), replication
				, GetDefaultBlockSize(f));
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
		public virtual FSDataOutputStream Create(Path f, short replication, Progressable 
			progress)
		{
			return Create(f, true, GetConf().GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey
				, CommonConfigurationKeysPublic.IoFileBufferSizeDefault), replication, GetDefaultBlockSize
				(f), progress);
		}

		/// <summary>Create an FSDataOutputStream at the indicated Path.</summary>
		/// <param name="f">the file name to create</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize)
		{
			return Create(f, overwrite, bufferSize, GetDefaultReplication(f), GetDefaultBlockSize
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
		public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
			Progressable progress)
		{
			return Create(f, overwrite, bufferSize, GetDefaultReplication(f), GetDefaultBlockSize
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
		public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
			short replication, long blockSize)
		{
			return Create(f, overwrite, bufferSize, replication, blockSize, null);
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
		public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
			short replication, long blockSize, Progressable progress)
		{
			return this.Create(f, FsPermission.GetFileDefault().ApplyUMask(FsPermission.GetUMask
				(GetConf())), overwrite, bufferSize, replication, blockSize, progress);
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
		/// <seealso cref="SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"
		/// 	/>
		public abstract FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress);

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
		/// <seealso cref="SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"
		/// 	/>
		public virtual FSDataOutputStream Create(Path f, FsPermission permission, EnumSet
			<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable
			 progress)
		{
			return Create(f, permission, flags, bufferSize, replication, blockSize, progress, 
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
		/// <seealso cref="SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"
		/// 	/>
		public virtual FSDataOutputStream Create(Path f, FsPermission permission, EnumSet
			<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable
			 progress, Options.ChecksumOpt checksumOpt)
		{
			// Checksum options are ignored by default. The file systems that
			// implement checksum need to override this method. The full
			// support is currently only available in DFS.
			return Create(f, permission, flags.Contains(CreateFlag.Overwrite), bufferSize, replication
				, blockSize, progress);
		}

		/*.
		* This create has been added to support the FileContext that processes
		* the permission
		* with umask before calling this method.
		* This a temporary method added to support the transition from FileSystem
		* to FileContext for user applications.
		*/
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		protected internal virtual FSDataOutputStream PrimitiveCreate(Path f, FsPermission
			 absolutePermission, EnumSet<CreateFlag> flag, int bufferSize, short replication
			, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
		{
			bool pathExists = Exists(f);
			CreateFlag.Validate(f, pathExists, flag);
			// Default impl  assumes that permissions do not matter and 
			// nor does the bytesPerChecksum  hence
			// calling the regular create is good enough.
			// FSs that implement permissions should override this.
			if (pathExists && flag.Contains(CreateFlag.Append))
			{
				return Append(f, bufferSize, progress);
			}
			return this.Create(f, absolutePermission, flag.Contains(CreateFlag.Overwrite), bufferSize
				, replication, blockSize, progress);
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
		[Obsolete]
		protected internal virtual bool PrimitiveMkdir(Path f, FsPermission absolutePermission
			)
		{
			// Default impl is to assume that permissions do not matter and hence
			// calling the regular mkdirs is good enough.
			// FSs that implement permissions should override this.
			return this.Mkdirs(f, absolutePermission);
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
		[Obsolete]
		protected internal virtual void PrimitiveMkdir(Path f, FsPermission absolutePermission
			, bool createParent)
		{
			if (!createParent)
			{
				// parent must exist.
				// since the this.mkdirs makes parent dirs automatically
				// we must throw exception if parent does not exist.
				FileStatus stat = GetFileStatus(f.GetParent());
				if (stat == null)
				{
					throw new FileNotFoundException("Missing parent:" + f);
				}
				if (!stat.IsDirectory())
				{
					throw new ParentNotDirectoryException("parent is not a dir");
				}
			}
			// parent does exist - go ahead with mkdir of leaf
			// Default impl is to assume that permissions do not matter and hence
			// calling the regular mkdirs is good enough.
			// FSs that implement permissions should override this.
			if (!this.Mkdirs(f, absolutePermission))
			{
				throw new IOException("mkdir of " + f + " failed");
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
		/// <seealso cref="SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"
		/// 	/>
		[Obsolete(@"API only for 0.20-append")]
		public virtual FSDataOutputStream CreateNonRecursive(Path f, bool overwrite, int 
			bufferSize, short replication, long blockSize, Progressable progress)
		{
			return this.CreateNonRecursive(f, FsPermission.GetFileDefault(), overwrite, bufferSize
				, replication, blockSize, progress);
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
		/// <seealso cref="SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"
		/// 	/>
		[Obsolete(@"API only for 0.20-append")]
		public virtual FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, bool overwrite, int bufferSize, short replication, long blockSize, Progressable
			 progress)
		{
			return CreateNonRecursive(f, permission, overwrite ? EnumSet.Of(CreateFlag.Create
				, CreateFlag.Overwrite) : EnumSet.Of(CreateFlag.Create), bufferSize, replication
				, blockSize, progress);
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
		/// <seealso cref="SetPermission(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"
		/// 	/>
		[Obsolete(@"API only for 0.20-append")]
		public virtual FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
			Progressable progress)
		{
			throw new IOException("createNonRecursive unsupported for this filesystem " + this
				.GetType());
		}

		/// <summary>Creates the given Path as a brand-new zero-length file.</summary>
		/// <remarks>
		/// Creates the given Path as a brand-new zero-length file.  If
		/// create fails, or if it already existed, return false.
		/// </remarks>
		/// <param name="f">path to use for create</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool CreateNewFile(Path f)
		{
			if (Exists(f))
			{
				return false;
			}
			else
			{
				Create(f, false, GetConf().GetInt("io.file.buffer.size", 4096)).Close();
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
		public virtual FSDataOutputStream Append(Path f)
		{
			return Append(f, GetConf().GetInt("io.file.buffer.size", 4096), null);
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <remarks>
		/// Append to an existing file (optional operation).
		/// Same as append(f, bufferSize, null).
		/// </remarks>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Append(Path f, int bufferSize)
		{
			return Append(f, bufferSize, null);
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="progress">for reporting progress if it is not null.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			);

		/// <summary>Concat existing files together.</summary>
		/// <param name="trg">the path to the target destination.</param>
		/// <param name="psrcs">the paths to the sources to use for the concatenation.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Concat(Path trg, Path[] psrcs)
		{
			throw new NotSupportedException("Not implemented by the " + GetType().Name + " FileSystem implementation"
				);
		}

		/// <summary>Get replication.</summary>
		/// <param name="src">file name</param>
		/// <returns>file replication</returns>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete(@"Use getFileStatus() instead")]
		public virtual short GetReplication(Path src)
		{
			return GetFileStatus(src).GetReplication();
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		public virtual bool SetReplication(Path src, short replication)
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
		public abstract bool Rename(Path src, Path dst);

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
		[Obsolete]
		protected internal virtual void Rename(Path src, Path dst, params Options.Rename[]
			 options)
		{
			// Default implementation
			FileStatus srcStatus = GetFileLinkStatus(src);
			if (srcStatus == null)
			{
				throw new FileNotFoundException("rename source " + src + " not found.");
			}
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
				if (srcStatus.IsDirectory() != dstStatus.IsDirectory())
				{
					throw new IOException("Source " + src + " Destination " + dst + " both should be either file or directory"
						);
				}
				if (!overwrite)
				{
					throw new FileAlreadyExistsException("rename destination " + dst + " already exists."
						);
				}
				// Delete the destination that is a file or an empty directory
				if (dstStatus.IsDirectory())
				{
					FileStatus[] list = ListStatus(dst);
					if (list != null && list.Length != 0)
					{
						throw new IOException("rename cannot overwrite non empty destination directory " 
							+ dst);
					}
				}
				Delete(dst, false);
			}
			else
			{
				Path parent = dst.GetParent();
				FileStatus parentStatus = GetFileStatus(parent);
				if (parentStatus == null)
				{
					throw new FileNotFoundException("rename destination parent " + parent + " not found."
						);
				}
				if (!parentStatus.IsDirectory())
				{
					throw new ParentNotDirectoryException("rename destination parent " + parent + " is a file."
						);
				}
			}
			if (!Rename(src, dst))
			{
				throw new IOException("rename from " + src + " to " + dst + " failed.");
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
		public virtual bool Truncate(Path f, long newLength)
		{
			throw new NotSupportedException("Not implemented by the " + GetType().Name + " FileSystem implementation"
				);
		}

		/// <summary>Delete a file</summary>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete(@"Use Delete(Path, bool) instead.")]
		public virtual bool Delete(Path f)
		{
			return Delete(f, true);
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
		public abstract bool Delete(Path f, bool recursive);

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
		public virtual bool DeleteOnExit(Path f)
		{
			if (!Exists(f))
			{
				return false;
			}
			lock (deleteOnExit)
			{
				deleteOnExit.AddItem(f);
			}
			return true;
		}

		/// <summary>Cancel the deletion of the path when the FileSystem is closed</summary>
		/// <param name="f">the path to cancel deletion</param>
		public virtual bool CancelDeleteOnExit(Path f)
		{
			lock (deleteOnExit)
			{
				return deleteOnExit.Remove(f);
			}
		}

		/// <summary>Delete all files that were marked as delete-on-exit.</summary>
		/// <remarks>
		/// Delete all files that were marked as delete-on-exit. This recursively
		/// deletes all files in the specified paths.
		/// </remarks>
		protected internal virtual void ProcessDeleteOnExit()
		{
			lock (deleteOnExit)
			{
				for (IEnumerator<Path> iter = deleteOnExit.GetEnumerator(); iter.HasNext(); )
				{
					Path path = iter.Next();
					try
					{
						if (Exists(path))
						{
							Delete(path, true);
						}
					}
					catch (IOException)
					{
						Log.Info("Ignoring failure to deleteOnExit for path " + path);
					}
					iter.Remove();
				}
			}
		}

		/// <summary>Check if exists.</summary>
		/// <param name="f">source file</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Exists(Path f)
		{
			try
			{
				return GetFileStatus(f) != null;
			}
			catch (FileNotFoundException)
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
		public virtual bool IsDirectory(Path f)
		{
			try
			{
				return GetFileStatus(f).IsDirectory();
			}
			catch (FileNotFoundException)
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
		public virtual bool IsFile(Path f)
		{
			try
			{
				return GetFileStatus(f).IsFile();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		// f does not exist
		/// <exception cref="System.IO.IOException"/>
		[Obsolete(@"Use getFileStatus() instead")]
		public virtual long GetLength(Path f)
		{
			return GetFileStatus(f).GetLen();
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
		public virtual ContentSummary GetContentSummary(Path f)
		{
			FileStatus status = GetFileStatus(f);
			if (status.IsFile())
			{
				// f is a file
				long length = status.GetLen();
				return new ContentSummary.Builder().Length(length).FileCount(1).DirectoryCount(0)
					.SpaceConsumed(length).Build();
			}
			// f is a directory
			long[] summary = new long[] { 0, 0, 1 };
			foreach (FileStatus s in ListStatus(f))
			{
				long length = s.GetLen();
				ContentSummary c = s.IsDirectory() ? GetContentSummary(s.GetPath()) : new ContentSummary.Builder
					().Length(length).FileCount(1).DirectoryCount(0).SpaceConsumed(length).Build();
				summary[0] += c.GetLength();
				summary[1] += c.GetFileCount();
				summary[2] += c.GetDirectoryCount();
			}
			return new ContentSummary.Builder().Length(summary[0]).FileCount(summary[1]).DirectoryCount
				(summary[2]).SpaceConsumed(summary[0]).Build();
		}

		private sealed class _PathFilter_1490 : PathFilter
		{
			public _PathFilter_1490()
			{
			}

			public bool Accept(Path file)
			{
				return true;
			}
		}

		private static readonly PathFilter DefaultFilter = new _PathFilter_1490();

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given patch</returns>
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract FileStatus[] ListStatus(Path f);

		/*
		* Filter files/directories in the given path using the user-supplied path
		* filter. Results are added to the given array <code>results</code>.
		*/
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ListStatus(AList<FileStatus> results, Path f, PathFilter filter)
		{
			FileStatus[] listing = ListStatus(f);
			if (listing == null)
			{
				throw new IOException("Error accessing " + f);
			}
			for (int i = 0; i < listing.Length; i++)
			{
				if (filter.Accept(listing[i].GetPath()))
				{
					results.AddItem(listing[i]);
				}
			}
		}

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
		/// Filter files/directories in the given path using the user-supplied path
		/// filter.
		/// </summary>
		/// <param name="f">a path name</param>
		/// <param name="filter">the user-supplied path filter</param>
		/// <returns>
		/// an array of FileStatus objects for the files under the given path
		/// after applying the filter
		/// </returns>
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus[] ListStatus(Path f, PathFilter filter)
		{
			AList<FileStatus> results = new AList<FileStatus>();
			ListStatus(results, f, filter);
			return Sharpen.Collections.ToArray(results, new FileStatus[results.Count]);
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
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus[] ListStatus(Path[] files)
		{
			return ListStatus(files, DefaultFilter);
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
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus[] ListStatus(Path[] files, PathFilter filter)
		{
			AList<FileStatus> results = new AList<FileStatus>();
			for (int i = 0; i < files.Length; i++)
			{
				ListStatus(results, files[i], filter);
			}
			return Sharpen.Collections.ToArray(results, new FileStatus[results.Count]);
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
		public virtual FileStatus[] GlobStatus(Path pathPattern)
		{
			return new Globber(this, pathPattern, DefaultFilter).Glob();
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
		public virtual FileStatus[] GlobStatus(Path pathPattern, PathFilter filter)
		{
			return new Globber(this, pathPattern, filter).Glob();
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
		/// <exception cref="System.IO.FileNotFoundException">If <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		public virtual RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path f)
		{
			return ListLocatedStatus(f, DefaultFilter);
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
		/// <exception cref="System.IO.FileNotFoundException">if <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if any I/O error occurred</exception>
		protected internal virtual RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path
			 f, PathFilter filter)
		{
			return new _RemoteIterator_1711(this, f, filter);
		}

		private sealed class _RemoteIterator_1711 : RemoteIterator<LocatedFileStatus>
		{
			public _RemoteIterator_1711(FileSystem _enclosing, Path f, PathFilter filter)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.filter = filter;
				this.stats = this._enclosing.ListStatus(f, filter);
				this.i = 0;
			}

			private readonly FileStatus[] stats;

			private int i;

			public bool HasNext()
			{
				return this.i < this.stats.Length;
			}

			/// <exception cref="System.IO.IOException"/>
			public LocatedFileStatus Next()
			{
				if (!this.HasNext())
				{
					throw new NoSuchElementException("No more entry in " + f);
				}
				FileStatus result = this.stats[this.i++];
				BlockLocation[] locs = result.IsFile() ? this._enclosing.GetFileBlockLocations(result
					.GetPath(), 0, result.GetLen()) : null;
				return new LocatedFileStatus(result, locs);
			}

			private readonly FileSystem _enclosing;

			private readonly Path f;

			private readonly PathFilter filter;
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
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<FileStatus> ListStatusIterator(Path p)
		{
			return new _RemoteIterator_1745(this, p);
		}

		private sealed class _RemoteIterator_1745 : RemoteIterator<FileStatus>
		{
			public _RemoteIterator_1745(Path p)
			{
				this.p = p;
				this.stats = this._enclosing.ListStatus(p);
				this.i = 0;
			}

			private readonly FileStatus[] stats;

			private int i;

			public bool HasNext()
			{
				return this.i < this.stats.Length;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileStatus Next()
			{
				if (!this.HasNext())
				{
					throw new NoSuchElementException("No more entry in " + p);
				}
				return this.stats[this.i++];
			}

			private readonly Path p;
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
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<LocatedFileStatus> ListFiles(Path f, bool recursive
			)
		{
			return new _RemoteIterator_1783(this, f, recursive);
		}

		private sealed class _RemoteIterator_1783 : RemoteIterator<LocatedFileStatus>
		{
			public _RemoteIterator_1783(FileSystem _enclosing, Path f, bool recursive)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.recursive = recursive;
				this.itors = new Stack<RemoteIterator<LocatedFileStatus>>();
				this.curItor = this._enclosing.ListLocatedStatus(f);
			}

			private Stack<RemoteIterator<LocatedFileStatus>> itors;

			private RemoteIterator<LocatedFileStatus> curItor;

			private LocatedFileStatus curFile;

			/// <exception cref="System.IO.IOException"/>
			public bool HasNext()
			{
				while (this.curFile == null)
				{
					if (this.curItor.HasNext())
					{
						this.HandleFileStat(this.curItor.Next());
					}
					else
					{
						if (!this.itors.Empty())
						{
							this.curItor = this.itors.Pop();
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
			private void HandleFileStat(LocatedFileStatus stat)
			{
				if (stat.IsFile())
				{
					// file
					this.curFile = stat;
				}
				else
				{
					if (recursive)
					{
						// directory
						this.itors.Push(this.curItor);
						this.curItor = this._enclosing.ListLocatedStatus(stat.GetPath());
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public LocatedFileStatus Next()
			{
				if (this.HasNext())
				{
					LocatedFileStatus result = this.curFile;
					this.curFile = null;
					return result;
				}
				throw new NoSuchElementException("No more entry in " + f);
			}

			private readonly FileSystem _enclosing;

			private readonly Path f;

			private readonly bool recursive;
		}

		/// <summary>Return the current user's home directory in this filesystem.</summary>
		/// <remarks>
		/// Return the current user's home directory in this filesystem.
		/// The default implementation returns "/user/$USER/".
		/// </remarks>
		public virtual Path GetHomeDirectory()
		{
			return this.MakeQualified(new Path("/user/" + Runtime.GetProperty("user.name")));
		}

		/// <summary>Set the current working directory for the given file system.</summary>
		/// <remarks>
		/// Set the current working directory for the given file system. All relative
		/// paths will be resolved relative to it.
		/// </remarks>
		/// <param name="new_dir"/>
		public abstract void SetWorkingDirectory(Path new_dir);

		/// <summary>Get the current working directory for the given file system</summary>
		/// <returns>the directory pathname</returns>
		public abstract Path GetWorkingDirectory();

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
		protected internal virtual Path GetInitialWorkingDirectory()
		{
			return null;
		}

		/// <summary>
		/// Call
		/// <see cref="Mkdirs(Path, Org.Apache.Hadoop.FS.Permission.FsPermission)"/>
		/// with default permission.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Mkdirs(Path f)
		{
			return Mkdirs(f, FsPermission.GetDirDefault());
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
		public abstract bool Mkdirs(Path f, FsPermission permission);

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name and the source is kept intact afterwards
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CopyFromLocalFile(Path src, Path dst)
		{
			CopyFromLocalFile(false, src, dst);
		}

		/// <summary>The src files is on the local disk.</summary>
		/// <remarks>
		/// The src files is on the local disk.  Add it to FS at
		/// the given dst name, removing the source afterwards.
		/// </remarks>
		/// <param name="srcs">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void MoveFromLocalFile(Path[] srcs, Path dst)
		{
			CopyFromLocalFile(true, true, srcs, dst);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name, removing the source afterwards.
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void MoveFromLocalFile(Path src, Path dst)
		{
			CopyFromLocalFile(true, src, dst);
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
		public virtual void CopyFromLocalFile(bool delSrc, Path src, Path dst)
		{
			CopyFromLocalFile(delSrc, true, src, dst);
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
		public virtual void CopyFromLocalFile(bool delSrc, bool overwrite, Path[] srcs, Path
			 dst)
		{
			Configuration conf = GetConf();
			FileUtil.Copy(GetLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
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
		public virtual void CopyFromLocalFile(bool delSrc, bool overwrite, Path src, Path
			 dst)
		{
			Configuration conf = GetConf();
			FileUtil.Copy(GetLocal(conf), src, this, dst, delSrc, overwrite, conf);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// </remarks>
		/// <param name="src">path</param>
		/// <param name="dst">path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CopyToLocalFile(Path src, Path dst)
		{
			CopyToLocalFile(false, src, dst);
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
		public virtual void MoveToLocalFile(Path src, Path dst)
		{
			CopyToLocalFile(true, src, dst);
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
		public virtual void CopyToLocalFile(bool delSrc, Path src, Path dst)
		{
			CopyToLocalFile(delSrc, src, dst, false);
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
		public virtual void CopyToLocalFile(bool delSrc, Path src, Path dst, bool useRawLocalFileSystem
			)
		{
			Configuration conf = GetConf();
			FileSystem local = null;
			if (useRawLocalFileSystem)
			{
				local = GetLocal(conf).GetRawFileSystem();
			}
			else
			{
				local = GetLocal(conf);
			}
			FileUtil.Copy(this, src, local, dst, delSrc, conf);
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
		public virtual Path StartLocalOutput(Path fsOutputFile, Path tmpLocalFile)
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
		public virtual void CompleteLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			MoveFromLocalFile(tmpLocalFile, fsOutputFile);
		}

		/// <summary>No more filesystem operations are needed.</summary>
		/// <remarks>
		/// No more filesystem operations are needed.  Will
		/// release any held locks.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			// delete all files that were marked as delete-on-exit.
			ProcessDeleteOnExit();
			Cache.Remove(this.key, this);
		}

		/// <summary>Return the total size of all files in the filesystem.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetUsed()
		{
			long used = 0;
			FileStatus[] files = ListStatus(new Path("/"));
			foreach (FileStatus file in files)
			{
				used += file.GetLen();
			}
			return used;
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete(@"Use getFileStatus() instead")]
		public virtual long GetBlockSize(Path f)
		{
			return GetFileStatus(f).GetBlockSize();
		}

		/// <summary>
		/// Return the number of bytes that large input files should be optimally
		/// be split into to minimize i/o time.
		/// </summary>
		[Obsolete(@"use GetDefaultBlockSize(Path) instead")]
		public virtual long GetDefaultBlockSize()
		{
			// default to 32MB: large enough to minimize the impact of seeks
			return GetConf().GetLong("fs.local.block.size", 32 * 1024 * 1024);
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
		public virtual long GetDefaultBlockSize(Path f)
		{
			return GetDefaultBlockSize();
		}

		/// <summary>Get the default replication.</summary>
		[Obsolete(@"use GetDefaultReplication(Path) instead")]
		public virtual short GetDefaultReplication()
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
		public virtual short GetDefaultReplication(Path path)
		{
			return GetDefaultReplication();
		}

		/// <summary>Return a file status object that represents the path.</summary>
		/// <param name="f">The path we want information from</param>
		/// <returns>a FileStatus object</returns>
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract FileStatus GetFileStatus(Path f);

		/// <summary>Checks if the user can access a path.</summary>
		/// <remarks>
		/// Checks if the user can access a path.  The mode specifies which access
		/// checks to perform.  If the requested permissions are granted, then the
		/// method returns normally.  If access is denied, then the method throws an
		/// <see cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// .
		/// <p/>
		/// The default implementation of this method calls
		/// <see cref="GetFileStatus(Path)"/>
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
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// .
		/// </remarks>
		/// <param name="path">Path to check</param>
		/// <param name="mode">type of access to check</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">if the path does not exist</exception>
		/// <exception cref="System.IO.IOException">see specific implementation</exception>
		public virtual void Access(Path path, FsAction mode)
		{
			CheckAccessPermissions(this.GetFileStatus(path), mode);
		}

		/// <summary>
		/// This method provides the default implementation of
		/// <see cref="Access(Path, Org.Apache.Hadoop.FS.Permission.FsAction)"/>
		/// .
		/// </summary>
		/// <param name="stat">FileStatus to check</param>
		/// <param name="mode">type of access to check</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		[InterfaceAudience.Private]
		internal static void CheckAccessPermissions(FileStatus stat, FsAction mode)
		{
			FsPermission perm = stat.GetPermission();
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			string user = ugi.GetShortUserName();
			IList<string> groups = Arrays.AsList(ugi.GetGroupNames());
			if (user.Equals(stat.GetOwner()))
			{
				if (perm.GetUserAction().Implies(mode))
				{
					return;
				}
			}
			else
			{
				if (groups.Contains(stat.GetGroup()))
				{
					if (perm.GetGroupAction().Implies(mode))
					{
						return;
					}
				}
				else
				{
					if (perm.GetOtherAction().Implies(mode))
					{
						return;
					}
				}
			}
			throw new AccessControlException(string.Format("Permission denied: user=%s, path=\"%s\":%s:%s:%s%s"
				, user, stat.GetPath(), stat.GetOwner(), stat.GetGroup(), stat.IsDirectory() ? "d"
				 : "-", perm));
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.FixRelativePart(Path)"/>
		/// </summary>
		protected internal virtual Path FixRelativePart(Path p)
		{
			if (p.IsUriPathAbsolute())
			{
				return p;
			}
			else
			{
				return new Path(GetWorkingDirectory(), p);
			}
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.CreateSymlink(Path, Path, bool)"/>
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateSymlink(Path target, Path link, bool createParent)
		{
			// Supporting filesystems should override this method
			throw new NotSupportedException("Filesystem does not support symlinks!");
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.GetFileLinkStatus(Path)"/>
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus GetFileLinkStatus(Path f)
		{
			// Supporting filesystems should override this method
			return GetFileStatus(f);
		}

		/// <summary>
		/// See
		/// <see cref="AbstractFileSystem.SupportsSymlinks()"/>
		/// </summary>
		public virtual bool SupportsSymlinks()
		{
			return false;
		}

		/// <summary>
		/// See
		/// <see cref="FileContext.GetLinkTarget(Path)"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLinkTarget(Path f)
		{
			// Supporting filesystems should override this method
			throw new NotSupportedException("Filesystem does not support symlinks!");
		}

		/// <summary>
		/// See
		/// <see cref="AbstractFileSystem.GetLinkTarget(Path)"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Path ResolveLink(Path f)
		{
			// Supporting filesystems should override this method
			throw new NotSupportedException("Filesystem does not support symlinks!");
		}

		/// <summary>Get the checksum of a file.</summary>
		/// <param name="f">The file path</param>
		/// <returns>
		/// The file checksum.  The default return value is null,
		/// which indicates that no checksum algorithm is implemented
		/// in the corresponding FileSystem.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileChecksum GetFileChecksum(Path f)
		{
			return GetFileChecksum(f, long.MaxValue);
		}

		/// <summary>
		/// Get the checksum of a file, from the beginning of the file till the
		/// specific length.
		/// </summary>
		/// <param name="f">The file path</param>
		/// <param name="length">The length of the file range for checksum calculation</param>
		/// <returns>The file checksum.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileChecksum GetFileChecksum(Path f, long length)
		{
			return null;
		}

		/// <summary>Set the verify checksum flag.</summary>
		/// <remarks>
		/// Set the verify checksum flag. This is only applicable if the
		/// corresponding FileSystem supports checksum. By default doesn't do anything.
		/// </remarks>
		/// <param name="verifyChecksum"/>
		public virtual void SetVerifyChecksum(bool verifyChecksum)
		{
		}

		//doesn't do anything
		/// <summary>Set the write checksum flag.</summary>
		/// <remarks>
		/// Set the write checksum flag. This is only applicable if the
		/// corresponding FileSystem supports checksum. By default doesn't do anything.
		/// </remarks>
		/// <param name="writeChecksum"/>
		public virtual void SetWriteChecksum(bool writeChecksum)
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
		public virtual FsStatus GetStatus()
		{
			return GetStatus(null);
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
		public virtual FsStatus GetStatus(Path p)
		{
			return new FsStatus(long.MaxValue, 0, long.MaxValue);
		}

		/// <summary>Set permission of a path.</summary>
		/// <param name="p"/>
		/// <param name="permission"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetPermission(Path p, FsPermission permission)
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
		public virtual void SetOwner(Path p, string username, string groupname)
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
		public virtual void SetTimes(Path p, long mtime, long atime)
		{
		}

		/// <summary>Create a snapshot with a default name.</summary>
		/// <param name="path">The directory where snapshots will be taken.</param>
		/// <returns>the snapshot path.</returns>
		/// <exception cref="System.IO.IOException"/>
		public Path CreateSnapshot(Path path)
		{
			return CreateSnapshot(path, null);
		}

		/// <summary>Create a snapshot</summary>
		/// <param name="path">The directory where snapshots will be taken.</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <returns>the snapshot path.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path CreateSnapshot(Path path, string snapshotName)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support createSnapshot"
				);
		}

		/// <summary>Rename a snapshot</summary>
		/// <param name="path">The directory path where the snapshot was taken</param>
		/// <param name="snapshotOldName">Old name of the snapshot</param>
		/// <param name="snapshotNewName">New name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RenameSnapshot(Path path, string snapshotOldName, string snapshotNewName
			)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support renameSnapshot"
				);
		}

		/// <summary>Delete a snapshot of a directory</summary>
		/// <param name="path">The directory that the to-be-deleted snapshot belongs to</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteSnapshot(Path path, string snapshotName)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support deleteSnapshot"
				);
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

		/// <summary>Gets the ACL of a file or directory.</summary>
		/// <param name="path">Path to get</param>
		/// <returns>AclStatus describing the ACL of the file or directory</returns>
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
		public virtual byte[] GetXAttr(Path path, string name)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getXAttr");
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
		public virtual IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getXAttrs");
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
		public virtual IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			throw new NotSupportedException(GetType().Name + " doesn't support getXAttrs");
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

		private static volatile bool FileSystemsLoaded = false;

		private static readonly IDictionary<string, Type> ServiceFileSystems = new Dictionary
			<string, Type>();

		// making it volatile to be able to do a double checked locking
		private static void LoadFileSystems()
		{
			lock (typeof(FileSystem))
			{
				if (!FileSystemsLoaded)
				{
					ServiceLoader<FileSystem> serviceLoader = ServiceLoader.Load
						<FileSystem>();
					foreach (FileSystem fs in serviceLoader)
					{
						ServiceFileSystems[fs.GetScheme()] = fs.GetType();
					}
					FileSystemsLoaded = true;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static Type GetFileSystemClass(string scheme, Configuration conf)
		{
			if (!FileSystemsLoaded)
			{
				LoadFileSystems();
			}
			Type clazz = null;
			if (conf != null)
			{
				clazz = (Type)conf.GetClass("fs." + scheme + ".impl", null);
			}
			if (clazz == null)
			{
				clazz = ServiceFileSystems[scheme];
			}
			if (clazz == null)
			{
				throw new IOException("No FileSystem for scheme: " + scheme);
			}
			return clazz;
		}

		/// <exception cref="System.IO.IOException"/>
		private static FileSystem CreateFileSystem(URI uri, Configuration
			 conf)
		{
			Type clazz = GetFileSystemClass(uri.GetScheme(), conf);
			FileSystem fs = (FileSystem)ReflectionUtils
				.NewInstance(clazz, conf);
			fs.Initialize(uri, conf);
			return fs;
		}

		/// <summary>Caching FileSystem objects</summary>
		internal class Cache
		{
			private readonly FileSystem.Cache.ClientFinalizer clientFinalizer;

			private readonly IDictionary<FileSystem.Cache.Key, FileSystem> map = new Dictionary
				<FileSystem.Cache.Key, FileSystem>();

			private readonly ICollection<FileSystem.Cache.Key> toAutoClose = new HashSet<FileSystem.Cache.Key
				>();

			/// <summary>A variable that makes all objects in the cache unique</summary>
			private static AtomicLong unique = new AtomicLong(1);

			/// <exception cref="System.IO.IOException"/>
			internal virtual FileSystem Get(URI uri, Configuration conf)
			{
				FileSystem.Cache.Key key = new FileSystem.Cache.Key(uri, conf);
				return GetInternal(uri, conf, key);
			}

			/// <summary>The objects inserted into the cache using this method are all unique</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual FileSystem GetUnique(URI uri, Configuration conf)
			{
				FileSystem.Cache.Key key = new FileSystem.Cache.Key(uri, conf, unique.GetAndIncrement
					());
				return GetInternal(uri, conf, key);
			}

			/// <exception cref="System.IO.IOException"/>
			private FileSystem GetInternal(URI uri, Configuration conf, FileSystem.Cache.Key 
				key)
			{
				FileSystem fs;
				lock (this)
				{
					fs = map[key];
				}
				if (fs != null)
				{
					return fs;
				}
				fs = CreateFileSystem(uri, conf);
				lock (this)
				{
					// refetch the lock again
					FileSystem oldfs = map[key];
					if (oldfs != null)
					{
						// a file system is created while lock is releasing
						fs.Close();
						// close the new file system
						return oldfs;
					}
					// return the old file system
					// now insert the new file system into the map
					if (map.IsEmpty() && !ShutdownHookManager.Get().IsShutdownInProgress())
					{
						ShutdownHookManager.Get().AddShutdownHook(clientFinalizer, ShutdownHookPriority);
					}
					fs.key = key;
					map[key] = fs;
					if (conf.GetBoolean("fs.automatic.close", true))
					{
						toAutoClose.AddItem(key);
					}
					return fs;
				}
			}

			internal virtual void Remove(FileSystem.Cache.Key key, FileSystem fs)
			{
				lock (this)
				{
					if (map.Contains(key) && fs == map[key])
					{
						Sharpen.Collections.Remove(map, key);
						toAutoClose.Remove(key);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CloseAll()
			{
				lock (this)
				{
					CloseAll(false);
				}
			}

			/// <summary>Close all FileSystem instances in the Cache.</summary>
			/// <param name="onlyAutomatic">only close those that are marked for automatic closing
			/// 	</param>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void CloseAll(bool onlyAutomatic)
			{
				lock (this)
				{
					IList<IOException> exceptions = new AList<IOException>();
					// Make a copy of the keys in the map since we'll be modifying
					// the map while iterating over it, which isn't safe.
					IList<FileSystem.Cache.Key> keys = new AList<FileSystem.Cache.Key>();
					Sharpen.Collections.AddAll(keys, map.Keys);
					foreach (FileSystem.Cache.Key key in keys)
					{
						FileSystem fs = map[key];
						if (onlyAutomatic && !toAutoClose.Contains(key))
						{
							continue;
						}
						//remove from cache
						Remove(key, fs);
						if (fs != null)
						{
							try
							{
								fs.Close();
							}
							catch (IOException ioe)
							{
								exceptions.AddItem(ioe);
							}
						}
					}
					if (!exceptions.IsEmpty())
					{
						throw MultipleIOException.CreateIOException(exceptions);
					}
				}
			}

			private class ClientFinalizer : Runnable
			{
				public virtual void Run()
				{
					lock (this)
					{
						try
						{
							this._enclosing.CloseAll(true);
						}
						catch (IOException e)
						{
							FileSystem.Log.Info("FileSystem.Cache.closeAll() threw an exception:\n" + e);
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
			internal virtual void CloseAll(UserGroupInformation ugi)
			{
				lock (this)
				{
					IList<FileSystem> targetFSList = new AList<FileSystem>();
					//Make a pass over the list and collect the filesystems to close
					//we cannot close inline since close() removes the entry from the Map
					foreach (KeyValuePair<FileSystem.Cache.Key, FileSystem> entry in map)
					{
						FileSystem.Cache.Key key = entry.Key;
						FileSystem fs = entry.Value;
						if (ugi.Equals(key.ugi) && fs != null)
						{
							targetFSList.AddItem(fs);
						}
					}
					IList<IOException> exceptions = new AList<IOException>();
					//now make a pass over the target list and close each
					foreach (FileSystem fs_1 in targetFSList)
					{
						try
						{
							fs_1.Close();
						}
						catch (IOException ioe)
						{
							exceptions.AddItem(ioe);
						}
					}
					if (!exceptions.IsEmpty())
					{
						throw MultipleIOException.CreateIOException(exceptions);
					}
				}
			}

			/// <summary>FileSystem.Cache.Key</summary>
			internal class Key
			{
				internal readonly string scheme;

				internal readonly string authority;

				internal readonly UserGroupInformation ugi;

				internal readonly long unique;

				/// <exception cref="System.IO.IOException"/>
				internal Key(URI uri, Configuration conf)
					: this(uri, conf, 0)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				internal Key(URI uri, Configuration conf, long unique)
				{
					// an artificial way to make a key unique
					scheme = uri.GetScheme() == null ? string.Empty : StringUtils.ToLowerCase(uri.GetScheme
						());
					authority = uri.GetAuthority() == null ? string.Empty : StringUtils.ToLowerCase(uri
						.GetAuthority());
					this.unique = unique;
					this.ugi = UserGroupInformation.GetCurrentUser();
				}

				public override int GetHashCode()
				{
					return (scheme + authority).GetHashCode() + ugi.GetHashCode() + (int)unique;
				}

				internal static bool IsEqual(object a, object b)
				{
					return a == b || (a != null && a.Equals(b));
				}

				public override bool Equals(object obj)
				{
					if (obj == this)
					{
						return true;
					}
					if (obj != null && obj is FileSystem.Cache.Key)
					{
						FileSystem.Cache.Key that = (FileSystem.Cache.Key)obj;
						return IsEqual(this.scheme, that.scheme) && IsEqual(this.authority, that.authority
							) && IsEqual(this.ugi, that.ugi) && (this.unique == that.unique);
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
				clientFinalizer = new FileSystem.Cache.ClientFinalizer(this);
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
				internal readonly WeakReference<Sharpen.Thread> owner;

				internal StatisticsData(WeakReference<Sharpen.Thread> owner)
				{
					this.owner = owner;
				}

				/// <summary>Add another StatisticsData object to this one.</summary>
				internal virtual void Add(FileSystem.Statistics.StatisticsData other)
				{
					this.bytesRead += other.bytesRead;
					this.bytesWritten += other.bytesWritten;
					this.readOps += other.readOps;
					this.largeReadOps += other.largeReadOps;
					this.writeOps += other.writeOps;
				}

				/// <summary>Negate the values of all statistics.</summary>
				internal virtual void Negate()
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

				public virtual long GetBytesRead()
				{
					return bytesRead;
				}

				public virtual long GetBytesWritten()
				{
					return bytesWritten;
				}

				public virtual int GetReadOps()
				{
					return readOps;
				}

				public virtual int GetLargeReadOps()
				{
					return largeReadOps;
				}

				public virtual int GetWriteOps()
				{
					return writeOps;
				}
			}

			private interface StatisticsAggregator<T>
			{
				void Accept(FileSystem.Statistics.StatisticsData data);

				T Aggregate();
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
			private readonly FileSystem.Statistics.StatisticsData rootData;

			/// <summary>Thread-local data.</summary>
			private readonly ThreadLocal<FileSystem.Statistics.StatisticsData> threadData;

			/// <summary>List of all thread-local data areas.</summary>
			/// <remarks>List of all thread-local data areas.  Protected by the Statistics lock.</remarks>
			private List<FileSystem.Statistics.StatisticsData> allData;

			public Statistics(string scheme)
			{
				this.scheme = scheme;
				this.rootData = new FileSystem.Statistics.StatisticsData(null);
				this.threadData = new ThreadLocal<FileSystem.Statistics.StatisticsData>();
				this.allData = null;
			}

			/// <summary>Copy constructor.</summary>
			/// <param name="other">The input Statistics object which is cloned.</param>
			public Statistics(FileSystem.Statistics other)
			{
				this.scheme = other.scheme;
				this.rootData = new FileSystem.Statistics.StatisticsData(null);
				other.VisitAll(new _StatisticsAggregator_2979(this));
				this.threadData = new ThreadLocal<FileSystem.Statistics.StatisticsData>();
			}

			private sealed class _StatisticsAggregator_2979 : FileSystem.Statistics.StatisticsAggregator
				<Void>
			{
				public _StatisticsAggregator_2979(Statistics _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this._enclosing.rootData.Add(data);
				}

				public Void Aggregate()
				{
					return null;
				}

				private readonly Statistics _enclosing;
			}

			/// <summary>Get or create the thread-local data associated with the current thread.</summary>
			public FileSystem.Statistics.StatisticsData GetThreadStatistics()
			{
				FileSystem.Statistics.StatisticsData data = threadData.Get();
				if (data == null)
				{
					data = new FileSystem.Statistics.StatisticsData(new WeakReference<Sharpen.Thread>
						(Sharpen.Thread.CurrentThread()));
					threadData.Set(data);
					lock (this)
					{
						if (allData == null)
						{
							allData = new List<FileSystem.Statistics.StatisticsData>();
						}
						allData.AddItem(data);
					}
				}
				return data;
			}

			/// <summary>Increment the bytes read in the statistics</summary>
			/// <param name="newBytes">the additional bytes read</param>
			public void IncrementBytesRead(long newBytes)
			{
				GetThreadStatistics().bytesRead += newBytes;
			}

			/// <summary>Increment the bytes written in the statistics</summary>
			/// <param name="newBytes">the additional bytes written</param>
			public void IncrementBytesWritten(long newBytes)
			{
				GetThreadStatistics().bytesWritten += newBytes;
			}

			/// <summary>Increment the number of read operations</summary>
			/// <param name="count">number of read operations</param>
			public void IncrementReadOps(int count)
			{
				GetThreadStatistics().readOps += count;
			}

			/// <summary>Increment the number of large read operations</summary>
			/// <param name="count">number of large read operations</param>
			public void IncrementLargeReadOps(int count)
			{
				GetThreadStatistics().largeReadOps += count;
			}

			/// <summary>Increment the number of write operations</summary>
			/// <param name="count">number of write operations</param>
			public void IncrementWriteOps(int count)
			{
				GetThreadStatistics().writeOps += count;
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
			private T VisitAll<T>(FileSystem.Statistics.StatisticsAggregator<T> visitor)
			{
				lock (this)
				{
					visitor.Accept(rootData);
					if (allData != null)
					{
						for (IEnumerator<FileSystem.Statistics.StatisticsData> iter = allData.GetEnumerator
							(); iter.HasNext(); )
						{
							FileSystem.Statistics.StatisticsData data = iter.Next();
							visitor.Accept(data);
							if (data.owner.Get() == null)
							{
								/*
								* If the thread that created this thread-local data no
								* longer exists, remove the StatisticsData from our list
								* and fold the values into rootData.
								*/
								rootData.Add(data);
								iter.Remove();
							}
						}
					}
					return visitor.Aggregate();
				}
			}

			/// <summary>Get the total number of bytes read</summary>
			/// <returns>the number of bytes</returns>
			public long GetBytesRead()
			{
				return VisitAll(new _StatisticsAggregator_3087());
			}

			private sealed class _StatisticsAggregator_3087 : FileSystem.Statistics.StatisticsAggregator
				<long>
			{
				public _StatisticsAggregator_3087()
				{
					this.bytesRead = 0;
				}

				private long bytesRead;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.bytesRead += data.bytesRead;
				}

				public long Aggregate()
				{
					return this.bytesRead;
				}
			}

			/// <summary>Get the total number of bytes written</summary>
			/// <returns>the number of bytes</returns>
			public long GetBytesWritten()
			{
				return VisitAll(new _StatisticsAggregator_3106());
			}

			private sealed class _StatisticsAggregator_3106 : FileSystem.Statistics.StatisticsAggregator
				<long>
			{
				public _StatisticsAggregator_3106()
				{
					this.bytesWritten = 0;
				}

				private long bytesWritten;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.bytesWritten += data.bytesWritten;
				}

				public long Aggregate()
				{
					return this.bytesWritten;
				}
			}

			/// <summary>Get the number of file system read operations such as list files</summary>
			/// <returns>number of read operations</returns>
			public int GetReadOps()
			{
				return VisitAll(new _StatisticsAggregator_3125());
			}

			private sealed class _StatisticsAggregator_3125 : FileSystem.Statistics.StatisticsAggregator
				<int>
			{
				public _StatisticsAggregator_3125()
				{
					this.readOps = 0;
				}

				private int readOps;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.readOps += data.readOps;
					this.readOps += data.largeReadOps;
				}

				public int Aggregate()
				{
					return this.readOps;
				}
			}

			/// <summary>
			/// Get the number of large file system read operations such as list files
			/// under a large directory
			/// </summary>
			/// <returns>number of large read operations</returns>
			public int GetLargeReadOps()
			{
				return VisitAll(new _StatisticsAggregator_3146());
			}

			private sealed class _StatisticsAggregator_3146 : FileSystem.Statistics.StatisticsAggregator
				<int>
			{
				public _StatisticsAggregator_3146()
				{
					this.largeReadOps = 0;
				}

				private int largeReadOps;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.largeReadOps += data.largeReadOps;
				}

				public int Aggregate()
				{
					return this.largeReadOps;
				}
			}

			/// <summary>
			/// Get the number of file system write operations such as create, append
			/// rename etc.
			/// </summary>
			/// <returns>number of write operations</returns>
			public int GetWriteOps()
			{
				return VisitAll(new _StatisticsAggregator_3166());
			}

			private sealed class _StatisticsAggregator_3166 : FileSystem.Statistics.StatisticsAggregator
				<int>
			{
				public _StatisticsAggregator_3166()
				{
					this.writeOps = 0;
				}

				private int writeOps;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.writeOps += data.writeOps;
				}

				public int Aggregate()
				{
					return this.writeOps;
				}
			}

			public override string ToString()
			{
				return VisitAll(new _StatisticsAggregator_3183());
			}

			private sealed class _StatisticsAggregator_3183 : FileSystem.Statistics.StatisticsAggregator
				<string>
			{
				public _StatisticsAggregator_3183()
				{
					this.total = new FileSystem.Statistics.StatisticsData(null);
				}

				private FileSystem.Statistics.StatisticsData total;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.total.Add(data);
				}

				public string Aggregate()
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
			public void Reset()
			{
				VisitAll(new _StatisticsAggregator_3216(this));
			}

			private sealed class _StatisticsAggregator_3216 : FileSystem.Statistics.StatisticsAggregator
				<Void>
			{
				public _StatisticsAggregator_3216(Statistics _enclosing)
				{
					this._enclosing = _enclosing;
					this.total = new FileSystem.Statistics.StatisticsData(null);
				}

				private FileSystem.Statistics.StatisticsData total;

				public void Accept(FileSystem.Statistics.StatisticsData data)
				{
					this.total.Add(data);
				}

				public Void Aggregate()
				{
					this.total.Negate();
					this._enclosing.rootData.Add(this.total);
					return null;
				}

				private readonly Statistics _enclosing;
			}

			/// <summary>Get the uri scheme associated with this statistics object.</summary>
			/// <returns>the schema associated with this set of statistics</returns>
			public string GetScheme()
			{
				return scheme;
			}
		}

		/// <summary>Get the Map of Statistics object indexed by URI Scheme.</summary>
		/// <returns>a Map having a key as URI scheme and value as Statistics object</returns>
		[Obsolete(@"use GetAllStatistics() instead")]
		public static IDictionary<string, FileSystem.Statistics> GetStatistics()
		{
			lock (typeof(FileSystem))
			{
				IDictionary<string, FileSystem.Statistics> result = new Dictionary<string, FileSystem.Statistics
					>();
				foreach (FileSystem.Statistics stat in statisticsTable.Values)
				{
					result[stat.GetScheme()] = stat;
				}
				return result;
			}
		}

		/// <summary>Return the FileSystem classes that have Statistics</summary>
		public static IList<FileSystem.Statistics> GetAllStatistics()
		{
			lock (typeof(FileSystem))
			{
				return new AList<FileSystem.Statistics>(statisticsTable.Values);
			}
		}

		/// <summary>Get the statistics for a particular file system</summary>
		/// <param name="cls">the class to lookup</param>
		/// <returns>a statistics object</returns>
		public static FileSystem.Statistics GetStatistics(string scheme, Type cls)
		{
			lock (typeof(FileSystem))
			{
				FileSystem.Statistics result = statisticsTable[cls];
				if (result == null)
				{
					result = new FileSystem.Statistics(scheme);
					statisticsTable[cls] = result;
				}
				return result;
			}
		}

		/// <summary>Reset all statistics for all file systems</summary>
		public static void ClearStatistics()
		{
			lock (typeof(FileSystem))
			{
				foreach (FileSystem.Statistics stat in statisticsTable.Values)
				{
					stat.Reset();
				}
			}
		}

		/// <summary>Print all statistics for all file systems</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void PrintStatistics()
		{
			lock (typeof(FileSystem))
			{
				foreach (KeyValuePair<Type, FileSystem.Statistics> pair in statisticsTable)
				{
					System.Console.Out.WriteLine("  FileSystem " + pair.Key.FullName + ": " + pair.Value
						);
				}
			}
		}

		private static bool symlinksEnabled = false;

		private static Configuration conf = null;

		// Symlinks are temporarily disabled - see HADOOP-10020 and HADOOP-10052
		[VisibleForTesting]
		public static bool AreSymlinksEnabled()
		{
			return symlinksEnabled;
		}

		[VisibleForTesting]
		public static void EnableSymlinks()
		{
			symlinksEnabled = true;
		}
	}
}

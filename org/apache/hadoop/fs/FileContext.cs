using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// The FileContext class provides an interface to the application writer for
	/// using the Hadoop file system.
	/// </summary>
	/// <remarks>
	/// The FileContext class provides an interface to the application writer for
	/// using the Hadoop file system.
	/// It provides a set of methods for the usual operation: create, open,
	/// list, etc
	/// <p>
	/// <b> *** Path Names *** </b>
	/// <p>
	/// The Hadoop file system supports a URI name space and URI names.
	/// It offers a forest of file systems that can be referenced using fully
	/// qualified URIs.
	/// Two common Hadoop file systems implementations are
	/// <ul>
	/// <li> the local file system: file:///path
	/// <li> the hdfs file system hdfs://nnAddress:nnPort/path
	/// </ul>
	/// While URI names are very flexible, it requires knowing the name or address
	/// of the server. For convenience one often wants to access the default system
	/// in one's environment without knowing its name/address. This has an
	/// additional benefit that it allows one to change one's default fs
	/// (e.g. admin moves application from cluster1 to cluster2).
	/// <p>
	/// To facilitate this, Hadoop supports a notion of a default file system.
	/// The user can set his default file system, although this is
	/// typically set up for you in your environment via your default config.
	/// A default file system implies a default scheme and authority; slash-relative
	/// names (such as /for/bar) are resolved relative to that default FS.
	/// Similarly a user can also have working-directory-relative names (i.e. names
	/// not starting with a slash). While the working directory is generally in the
	/// same default FS, the wd can be in a different FS.
	/// <p>
	/// Hence Hadoop path names can be one of:
	/// <ul>
	/// <li> fully qualified URI: scheme://authority/path
	/// <li> slash relative names: /path relative to the default file system
	/// <li> wd-relative names: path  relative to the working dir
	/// </ul>
	/// Relative paths with scheme (scheme:foo/bar) are illegal.
	/// <p>
	/// <b>****The Role of the FileContext and configuration defaults****</b>
	/// <p>
	/// The FileContext provides file namespace context for resolving file names;
	/// it also contains the umask for permissions, In that sense it is like the
	/// per-process file-related state in Unix system.
	/// These two properties
	/// <ul>
	/// <li> default file system i.e your slash)
	/// <li> umask
	/// </ul>
	/// in general, are obtained from the default configuration file
	/// in your environment,  (@see
	/// <see cref="org.apache.hadoop.conf.Configuration"/>
	/// ).
	/// No other configuration parameters are obtained from the default config as
	/// far as the file context layer is concerned. All file system instances
	/// (i.e. deployments of file systems) have default properties; we call these
	/// server side (SS) defaults. Operation like create allow one to select many
	/// properties: either pass them in as explicit parameters or use
	/// the SS properties.
	/// <p>
	/// The file system related SS defaults are
	/// <ul>
	/// <li> the home directory (default is "/user/userName")
	/// <li> the initial wd (only for local fs)
	/// <li> replication factor
	/// <li> block size
	/// <li> buffer size
	/// <li> encryptDataTransfer
	/// <li> checksum option. (checksumType and  bytesPerChecksum)
	/// </ul>
	/// <p>
	/// <b> *** Usage Model for the FileContext class *** </b>
	/// <p>
	/// Example 1: use the default config read from the $HADOOP_CONFIG/core.xml.
	/// Unspecified values come from core-defaults.xml in the release jar.
	/// <ul>
	/// <li> myFContext = FileContext.getFileContext(); // uses the default config
	/// // which has your default FS
	/// <li>  myFContext.create(path, ...);
	/// <li>  myFContext.setWorkingDir(path)
	/// <li>  myFContext.open (path, ...);
	/// </ul>
	/// Example 2: Get a FileContext with a specific URI as the default FS
	/// <ul>
	/// <li> myFContext = FileContext.getFileContext(URI)
	/// <li> myFContext.create(path, ...);
	/// ...
	/// </ul>
	/// Example 3: FileContext with local file system as the default
	/// <ul>
	/// <li> myFContext = FileContext.getLocalFSFileContext()
	/// <li> myFContext.create(path, ...);
	/// <li> ...
	/// </ul>
	/// Example 4: Use a specific config, ignoring $HADOOP_CONFIG
	/// Generally you should not need use a config unless you are doing
	/// <ul>
	/// <li> configX = someConfigSomeOnePassedToYou.
	/// <li> myFContext = getFileContext(configX); // configX is not changed,
	/// // is passed down
	/// <li> myFContext.create(path, ...);
	/// <li>...
	/// </ul>
	/// </remarks>
	public class FileContext
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileContext)
			));

		/// <summary>
		/// Default permission for directory and symlink
		/// In previous versions, this default permission was also used to
		/// create files, so files created end up with ugo+x permission.
		/// </summary>
		/// <remarks>
		/// Default permission for directory and symlink
		/// In previous versions, this default permission was also used to
		/// create files, so files created end up with ugo+x permission.
		/// See HADOOP-9155 for detail.
		/// Two new constants are added to solve this, please use
		/// <see cref="DIR_DEFAULT_PERM"/>
		/// for directory, and use
		/// <see cref="FILE_DEFAULT_PERM"/>
		/// for file.
		/// This constant is kept for compatibility.
		/// </remarks>
		public static readonly org.apache.hadoop.fs.permission.FsPermission DEFAULT_PERM = 
			org.apache.hadoop.fs.permission.FsPermission.getDefault();

		/// <summary>Default permission for directory</summary>
		public static readonly org.apache.hadoop.fs.permission.FsPermission DIR_DEFAULT_PERM
			 = org.apache.hadoop.fs.permission.FsPermission.getDirDefault();

		/// <summary>Default permission for file</summary>
		public static readonly org.apache.hadoop.fs.permission.FsPermission FILE_DEFAULT_PERM
			 = org.apache.hadoop.fs.permission.FsPermission.getFileDefault();

		/// <summary>Priority of the FileContext shutdown hook.</summary>
		public const int SHUTDOWN_HOOK_PRIORITY = 20;

		/// <summary>List of files that should be deleted on JVM shutdown.</summary>
		internal static readonly System.Collections.Generic.IDictionary<org.apache.hadoop.fs.FileContext
			, System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path>> DELETE_ON_EXIT
			 = new java.util.IdentityHashMap<org.apache.hadoop.fs.FileContext, System.Collections.Generic.ICollection
			<org.apache.hadoop.fs.Path>>();

		/// <summary>JVM shutdown hook thread.</summary>
		internal static readonly org.apache.hadoop.fs.FileContext.FileContextFinalizer FINALIZER
			 = new org.apache.hadoop.fs.FileContext.FileContextFinalizer();

		private sealed class _PathFilter_212 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_212()
			{
			}

			/*Evolving for a release,to be changed to Stable */
			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return true;
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter DEFAULT_FILTER = new _PathFilter_212
			();

		/// <summary>The FileContext is defined by.</summary>
		/// <remarks>
		/// The FileContext is defined by.
		/// 1) defaultFS (slash)
		/// 2) wd
		/// 3) umask
		/// </remarks>
		private readonly org.apache.hadoop.fs.AbstractFileSystem defaultFS;

		private org.apache.hadoop.fs.Path workingDir;

		private org.apache.hadoop.fs.permission.FsPermission umask;

		private readonly org.apache.hadoop.conf.Configuration conf;

		private readonly org.apache.hadoop.security.UserGroupInformation ugi;

		internal readonly bool resolveSymlinks;

		private FileContext(org.apache.hadoop.fs.AbstractFileSystem defFs, org.apache.hadoop.fs.permission.FsPermission
			 theUmask, org.apache.hadoop.conf.Configuration aConf)
		{
			//default FS for this FileContext.
			// Fully qualified
			defaultFS = defFs;
			umask = org.apache.hadoop.fs.permission.FsPermission.getUMask(aConf);
			conf = aConf;
			try
			{
				ugi = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
			}
			catch (System.IO.IOException e)
			{
				LOG.error("Exception in getCurrentUser: ", e);
				throw new System.Exception("Failed to get the current user " + "while creating a FileContext"
					, e);
			}
			/*
			* Init the wd.
			* WorkingDir is implemented at the FileContext layer
			* NOT at the AbstractFileSystem layer.
			* If the DefaultFS, such as localFilesystem has a notion of
			*  builtin WD, we use that as the initial WD.
			*  Otherwise the WD is initialized to the home directory.
			*/
			workingDir = defaultFS.getInitialWorkingDirectory();
			if (workingDir == null)
			{
				workingDir = defaultFS.getHomeDirectory();
			}
			resolveSymlinks = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT
				);
			util = new org.apache.hadoop.fs.FileContext.Util(this);
		}

		// for the inner class
		/*
		* Remove relative part - return "absolute":
		* If input is relative path ("foo/bar") add wd: ie "/<workingDir>/foo/bar"
		* A fully qualified uri ("hdfs://nn:p/foo/bar") or a slash-relative path
		* ("/foo/bar") are returned unchanged.
		*
		* Applications that use FileContext should use #makeQualified() since
		* they really want a fully qualified URI.
		* Hence this method is not called makeAbsolute() and
		* has been deliberately declared private.
		*/
		internal virtual org.apache.hadoop.fs.Path fixRelativePart(org.apache.hadoop.fs.Path
			 p)
		{
			if (p.isUriPathAbsolute())
			{
				return p;
			}
			else
			{
				return new org.apache.hadoop.fs.Path(workingDir, p);
			}
		}

		/// <summary>Delete all the paths that were marked as delete-on-exit.</summary>
		internal static void processDeleteOnExit()
		{
			lock (DELETE_ON_EXIT)
			{
				System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<org.apache.hadoop.fs.FileContext
					, System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path>>> set = DELETE_ON_EXIT;
				foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.fs.FileContext
					, System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path>> entry in set)
				{
					org.apache.hadoop.fs.FileContext fc = entry.Key;
					System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path> paths = entry.Value;
					foreach (org.apache.hadoop.fs.Path path in paths)
					{
						try
						{
							fc.delete(path, true);
						}
						catch (System.IO.IOException)
						{
							LOG.warn("Ignoring failure to deleteOnExit for path " + path);
						}
					}
				}
				DELETE_ON_EXIT.clear();
			}
		}

		/// <summary>Get the file system of supplied path.</summary>
		/// <param name="absOrFqPath">- absolute or fully qualified path</param>
		/// <returns>the file system of the path</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// If the file system for
		/// <code>absOrFqPath</code> is not supported.
		/// </exception>
		/// <exception cref="IOExcepton">
		/// If the file system for <code>absOrFqPath</code> could
		/// not be instantiated.
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.AbstractFileSystem getFSofPath(org.apache.hadoop.fs.Path
			 absOrFqPath)
		{
			absOrFqPath.checkNotSchemeWithRelative();
			absOrFqPath.checkNotRelative();
			try
			{
				// Is it the default FS for this FileContext?
				defaultFS.checkPath(absOrFqPath);
				return defaultFS;
			}
			catch (System.Exception)
			{
				// it is different FileSystem
				return getAbstractFileSystem(ugi, absOrFqPath.toUri(), conf);
			}
		}

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.AbstractFileSystem getAbstractFileSystem(org.apache.hadoop.security.UserGroupInformation
			 user, java.net.URI uri, org.apache.hadoop.conf.Configuration conf)
		{
			try
			{
				return user.doAs(new _PrivilegedExceptionAction_331(uri, conf));
			}
			catch (System.Exception ex)
			{
				LOG.error(ex);
				throw new System.IO.IOException("Failed to get the AbstractFileSystem for path: "
					 + uri, ex);
			}
		}

		private sealed class _PrivilegedExceptionAction_331 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.AbstractFileSystem>
		{
			public _PrivilegedExceptionAction_331(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public org.apache.hadoop.fs.AbstractFileSystem run()
			{
				return org.apache.hadoop.fs.AbstractFileSystem.get(uri, conf);
			}

			private readonly java.net.URI uri;

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <summary>
		/// Create a FileContext with specified FS as default using the specified
		/// config.
		/// </summary>
		/// <param name="defFS"/>
		/// <param name="aConf"/>
		/// <returns>new FileContext with specifed FS as default.</returns>
		public static org.apache.hadoop.fs.FileContext getFileContext(org.apache.hadoop.fs.AbstractFileSystem
			 defFS, org.apache.hadoop.conf.Configuration aConf)
		{
			return new org.apache.hadoop.fs.FileContext(defFS, org.apache.hadoop.fs.permission.FsPermission
				.getUMask(aConf), aConf);
		}

		/// <summary>Create a FileContext for specified file system using the default config.
		/// 	</summary>
		/// <param name="defaultFS"/>
		/// <returns>
		/// a FileContext with the specified AbstractFileSystem
		/// as the default FS.
		/// </returns>
		protected internal static org.apache.hadoop.fs.FileContext getFileContext(org.apache.hadoop.fs.AbstractFileSystem
			 defaultFS)
		{
			return getFileContext(defaultFS, new org.apache.hadoop.conf.Configuration());
		}

		/// <summary>
		/// Create a FileContext using the default config read from the
		/// $HADOOP_CONFIG/core.xml, Unspecified key-values for config are defaulted
		/// from core-defaults.xml in the release jar.
		/// </summary>
		/// <exception cref="UnsupportedFileSystemException">
		/// If the file system from the default
		/// configuration is not supported
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.FileContext getFileContext()
		{
			return getFileContext(new org.apache.hadoop.conf.Configuration());
		}

		/// <returns>a FileContext for the local file system using the default config.</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// If the file system for
		/// <see cref="FsConstants.LOCAL_FS_URI"/>
		/// is not supported.
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.FileContext getLocalFSFileContext()
		{
			return getFileContext(org.apache.hadoop.fs.FsConstants.LOCAL_FS_URI);
		}

		/// <summary>Create a FileContext for specified URI using the default config.</summary>
		/// <param name="defaultFsUri"/>
		/// <returns>a FileContext with the specified URI as the default FS.</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// If the file system for
		/// <code>defaultFsUri</code> is not supported
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.FileContext getFileContext(java.net.URI defaultFsUri
			)
		{
			return getFileContext(defaultFsUri, new org.apache.hadoop.conf.Configuration());
		}

		/// <summary>Create a FileContext for specified default URI using the specified config.
		/// 	</summary>
		/// <param name="defaultFsUri"/>
		/// <param name="aConf"/>
		/// <returns>new FileContext for specified uri</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// If the file system with specified is
		/// not supported
		/// </exception>
		/// <exception cref="System.Exception">
		/// If the file system specified is supported but
		/// could not be instantiated, or if login fails.
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.FileContext getFileContext(java.net.URI defaultFsUri
			, org.apache.hadoop.conf.Configuration aConf)
		{
			org.apache.hadoop.security.UserGroupInformation currentUser = null;
			org.apache.hadoop.fs.AbstractFileSystem defaultAfs = null;
			if (defaultFsUri.getScheme() == null)
			{
				return getFileContext(aConf);
			}
			try
			{
				currentUser = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
				defaultAfs = getAbstractFileSystem(currentUser, defaultFsUri, aConf);
			}
			catch (org.apache.hadoop.fs.UnsupportedFileSystemException ex)
			{
				throw;
			}
			catch (System.IO.IOException ex)
			{
				LOG.error(ex);
				throw new System.Exception(ex);
			}
			return getFileContext(defaultAfs, aConf);
		}

		/// <summary>Create a FileContext using the passed config.</summary>
		/// <remarks>
		/// Create a FileContext using the passed config. Generally it is better to use
		/// <see cref="getFileContext(java.net.URI, org.apache.hadoop.conf.Configuration)"/>
		/// instead of this one.
		/// </remarks>
		/// <param name="aConf"/>
		/// <returns>new FileContext</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system in the config
		/// is not supported
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.FileContext getFileContext(org.apache.hadoop.conf.Configuration
			 aConf)
		{
			java.net.URI defaultFsUri = java.net.URI.create(aConf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.FS_DEFAULT_NAME_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT
				));
			if (defaultFsUri.getScheme() != null && !defaultFsUri.getScheme().Trim().isEmpty(
				))
			{
				return getFileContext(defaultFsUri, aConf);
			}
			throw new org.apache.hadoop.fs.UnsupportedFileSystemException(string.format("%s: URI configured via %s carries no scheme"
				, defaultFsUri, org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY
				));
		}

		/// <param name="aConf">- from which the FileContext is configured</param>
		/// <returns>a FileContext for the local file system using the specified config.</returns>
		/// <exception cref="UnsupportedFileSystemException">
		/// If default file system in the config
		/// is not supported
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public static org.apache.hadoop.fs.FileContext getLocalFSFileContext(org.apache.hadoop.conf.Configuration
			 aConf)
		{
			return getFileContext(org.apache.hadoop.fs.FsConstants.LOCAL_FS_URI, aConf);
		}

		/* This method is needed for tests. */
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		public virtual org.apache.hadoop.fs.AbstractFileSystem getDefaultFileSystem()
		{
			/* return type will change to AFS once
			HADOOP-6223 is completed */
			return defaultFS;
		}

		/// <summary>Set the working directory for wd-relative names (such a "foo/bar").</summary>
		/// <remarks>
		/// Set the working directory for wd-relative names (such a "foo/bar"). Working
		/// directory feature is provided by simply prefixing relative names with the
		/// working dir. Note this is different from Unix where the wd is actually set
		/// to the inode. Hence setWorkingDir does not follow symlinks etc. This works
		/// better in a distributed environment that has multiple independent roots.
		/// <see cref="getWorkingDirectory()"/>
		/// should return what setWorkingDir() set.
		/// </remarks>
		/// <param name="newWDir">new working directory</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// <br />
		/// NewWdir can be one of:
		/// <ul>
		/// <li>relative path: "foo/bar";</li>
		/// <li>absolute without scheme: "/foo/bar"</li>
		/// <li>fully qualified with scheme: "xx://auth/foo/bar"</li>
		/// </ul>
		/// <br />
		/// Illegal WDs:
		/// <ul>
		/// <li>relative with scheme: "xx:foo/bar"</li>
		/// <li>non existent directory</li>
		/// </ul>
		/// </exception>
		public virtual void setWorkingDirectory(org.apache.hadoop.fs.Path newWDir)
		{
			newWDir.checkNotSchemeWithRelative();
			/* wd is stored as a fully qualified path. We check if the given
			* path is not relative first since resolve requires and returns
			* an absolute path.
			*/
			org.apache.hadoop.fs.Path newWorkingDir = new org.apache.hadoop.fs.Path(workingDir
				, newWDir);
			org.apache.hadoop.fs.FileStatus status = getFileStatus(newWorkingDir);
			if (status.isFile())
			{
				throw new java.io.FileNotFoundException("Cannot setWD to a file");
			}
			workingDir = newWorkingDir;
		}

		/// <summary>Gets the working directory for wd-relative names (such a "foo/bar").</summary>
		public virtual org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return workingDir;
		}

		/// <summary>Gets the ugi in the file-context</summary>
		/// <returns>UserGroupInformation</returns>
		public virtual org.apache.hadoop.security.UserGroupInformation getUgi()
		{
			return ugi;
		}

		/// <summary>Return the current user's home directory in this file system.</summary>
		/// <remarks>
		/// Return the current user's home directory in this file system.
		/// The default implementation returns "/user/$USER/".
		/// </remarks>
		/// <returns>the home directory</returns>
		public virtual org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return defaultFS.getHomeDirectory();
		}

		/// <returns>the umask of this FileContext</returns>
		public virtual org.apache.hadoop.fs.permission.FsPermission getUMask()
		{
			return umask;
		}

		/// <summary>Set umask to the supplied parameter.</summary>
		/// <param name="newUmask">the new umask</param>
		public virtual void setUMask(org.apache.hadoop.fs.permission.FsPermission newUmask
			)
		{
			umask = newUmask;
		}

		/// <summary>Resolve the path following any symlinks or mount points</summary>
		/// <param name="f">to be resolved</param>
		/// <returns>fully qualified resolved path</returns>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if access denied
		/// 	</exception>
		/// <exception cref="System.IO.IOException">
		/// If an IO Error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="InvalidPathException">If path <code>f</code> is not valid</exception>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public virtual org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path f)
		{
			return resolve(f);
		}

		/// <summary>Make the path fully qualified if it is isn't.</summary>
		/// <remarks>
		/// Make the path fully qualified if it is isn't.
		/// A Fully-qualified path has scheme and authority specified and an absolute
		/// path.
		/// Use the default file system and working dir in this FileContext to qualify.
		/// </remarks>
		/// <param name="path"/>
		/// <returns>qualified path</returns>
		public virtual org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path 
			path)
		{
			return path.makeQualified(defaultFS.getUri(), getWorkingDirectory());
		}

		/// <summary>
		/// Create or overwrite file on indicated path and returns an output stream for
		/// writing into the file.
		/// </summary>
		/// <param name="f">the file name to open</param>
		/// <param name="createFlag">
		/// gives the semantics of create; see
		/// <see cref="CreateFlag"/>
		/// </param>
		/// <param name="opts">
		/// file creation options; see
		/// <see cref="CreateOpts"/>
		/// .
		/// <ul>
		/// <li>Progress - to report progress on the operation - default null
		/// <li>Permission - umask is applied against permisssion: default is
		/// FsPermissions:getDefault()
		/// <li>CreateParent - create missing parent path; default is to not
		/// to create parents
		/// <li>The defaults for the following are SS defaults of the file
		/// server implementing the target path. Not all parameters make sense
		/// for all kinds of file system - eg. localFS ignores Blocksize,
		/// replication, checksum
		/// <ul>
		/// <li>BufferSize - buffersize used in FSDataOutputStream
		/// <li>Blocksize - block size for file blocks
		/// <li>ReplicationFactor - replication for blocks
		/// <li>ChecksumParam - Checksum parameters. server default is used
		/// if not specified.
		/// </ul>
		/// </ul>
		/// </param>
		/// <returns>
		/// 
		/// <see cref="FSDataOutputStream"/>
		/// for created file
		/// </returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="FileAlreadyExistsException">If file <code>f</code> already exists
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">
		/// If parent of <code>f</code> does not exist
		/// and <code>createParent</code> is false
		/// </exception>
		/// <exception cref="ParentNotDirectoryException">
		/// If parent of <code>f</code> is not a
		/// directory.
		/// </exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="InvalidPathException">If path <code>f</code> is not valid</exception>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, params org.apache.hadoop.fs.Options.CreateOpts
			[] opts)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			// If one of the options is a permission, extract it & apply umask
			// If not, add a default Perms and apply umask;
			// AbstractFileSystem#create
			org.apache.hadoop.fs.Options.CreateOpts.Perms permOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.Perms>(opts);
			org.apache.hadoop.fs.permission.FsPermission permission = (permOpt != null) ? permOpt
				.getValue() : FILE_DEFAULT_PERM;
			permission = permission.applyUMask(umask);
			org.apache.hadoop.fs.Options.CreateOpts[] updatedOpts = org.apache.hadoop.fs.Options.CreateOpts
				.setOpt(org.apache.hadoop.fs.Options.CreateOpts.perms(permission), opts);
			return new _FSLinkResolver_682(createFlag, updatedOpts).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_682 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FSDataOutputStream
			>
		{
			public _FSLinkResolver_682(java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag
				, org.apache.hadoop.fs.Options.CreateOpts[] updatedOpts)
			{
				this.createFlag = createFlag;
				this.updatedOpts = updatedOpts;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.create(p, createFlag, updatedOpts);
			}

			private readonly java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag;

			private readonly org.apache.hadoop.fs.Options.CreateOpts[] updatedOpts;
		}

		/// <summary>Make(create) a directory and all the non-existent parents.</summary>
		/// <param name="dir">- the dir to make</param>
		/// <param name="permission">- permissions is set permission&~umask</param>
		/// <param name="createParent">
		/// - if true then missing parent dirs are created if false
		/// then parent must exist
		/// </param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="FileAlreadyExistsException">
		/// If directory <code>dir</code> already
		/// exists
		/// </exception>
		/// <exception cref="java.io.FileNotFoundException">
		/// If parent of <code>dir</code> does not exist
		/// and <code>createParent</code> is false
		/// </exception>
		/// <exception cref="ParentNotDirectoryException">
		/// If parent of <code>dir</code> is not a
		/// directory
		/// </exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>dir</code>
		/// is not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="InvalidPathException">If path <code>dir</code> is not valid</exception>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent)
		{
			org.apache.hadoop.fs.Path absDir = fixRelativePart(dir);
			org.apache.hadoop.fs.permission.FsPermission absFerms = (permission == null ? org.apache.hadoop.fs.permission.FsPermission
				.getDirDefault() : permission).applyUMask(umask);
			new _FSLinkResolver_726(absFerms, createParent).resolve(this, absDir);
		}

		private sealed class _FSLinkResolver_726 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_726(org.apache.hadoop.fs.permission.FsPermission absFerms, 
				bool createParent)
			{
				this.absFerms = absFerms;
				this.createParent = createParent;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.mkdir(p, absFerms, createParent);
				return null;
			}

			private readonly org.apache.hadoop.fs.permission.FsPermission absFerms;

			private readonly bool createParent;
		}

		/// <summary>Delete a file.</summary>
		/// <param name="f">the path to delete.</param>
		/// <param name="recursive">
		/// if path is a directory and set to
		/// true, the directory is deleted else throws an exception. In
		/// case of a file the recursive can be set to either true or false.
		/// </param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="InvalidPathException">If path <code>f</code> is invalid</exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_762(recursive).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_762 : org.apache.hadoop.fs.FSLinkResolver<bool
			>
		{
			public _FSLinkResolver_762(bool recursive)
			{
				this.recursive = recursive;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override bool next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				return bool.valueOf(fs.delete(p, recursive));
			}

			private readonly bool recursive;
		}

		/// <summary>
		/// Opens an FSDataInputStream at the indicated Path using
		/// default buffersize.
		/// </summary>
		/// <param name="f">the file name to open</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If file <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code>
		/// is not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_791().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_791 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FSDataInputStream
			>
		{
			public _FSLinkResolver_791()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FSDataInputStream next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.open(p);
			}
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If file <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_822(bufferSize).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_822 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FSDataInputStream
			>
		{
			public _FSLinkResolver_822(int bufferSize)
			{
				this.bufferSize = bufferSize;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FSDataInputStream next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.open(p, bufferSize);
			}

			private readonly int bufferSize;
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
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If file <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_865(newLength).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_865 : org.apache.hadoop.fs.FSLinkResolver<bool
			>
		{
			public _FSLinkResolver_865(long newLength)
			{
				this.newLength = newLength;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override bool next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				return fs.truncate(p, newLength);
			}

			private readonly long newLength;
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="f">file name</param>
		/// <param name="replication">new replication</param>
		/// <returns>true if successful</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If file <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		public virtual bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_896(replication).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_896 : org.apache.hadoop.fs.FSLinkResolver<bool
			>
		{
			public _FSLinkResolver_896(short replication)
			{
				this.replication = replication;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override bool next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				return bool.valueOf(fs.setReplication(p, replication));
			}

			private readonly short replication;
		}

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
		/// If OVERWRITE option is not passed as an argument, rename fails if the dst
		/// already exists.
		/// <p>
		/// If OVERWRITE option is passed as an argument, rename overwrites the dst if
		/// it is a file or an empty directory. Rename fails if dst is a non-empty
		/// directory.
		/// <p>
		/// Note that atomicity of rename is dependent on the file system
		/// implementation. Please refer to the file system documentation for details
		/// <p>
		/// </remarks>
		/// <param name="src">path to be renamed</param>
		/// <param name="dst">new path after rename</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="FileAlreadyExistsException">
		/// If <code>dst</code> already exists and
		/// <code>options</options> has
		/// <see cref="Rename.OVERWRITE"/>
		/// 
		/// option false.
		/// </exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>src</code> does not exist
		/// 	</exception>
		/// <exception cref="ParentNotDirectoryException">
		/// If parent of <code>dst</code> is not a
		/// directory
		/// </exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>src</code>
		/// and <code>dst</code> is not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, params org.apache.hadoop.fs.Options.Rename[] options)
		{
			org.apache.hadoop.fs.Path absSrc = fixRelativePart(src);
			org.apache.hadoop.fs.Path absDst = fixRelativePart(dst);
			org.apache.hadoop.fs.AbstractFileSystem srcFS = getFSofPath(absSrc);
			org.apache.hadoop.fs.AbstractFileSystem dstFS = getFSofPath(absDst);
			if (!srcFS.getUri().Equals(dstFS.getUri()))
			{
				throw new System.IO.IOException("Renames across AbstractFileSystems not supported"
					);
			}
			try
			{
				srcFS.rename(absSrc, absDst, options);
			}
			catch (org.apache.hadoop.fs.UnresolvedLinkException)
			{
				/* We do not know whether the source or the destination path
				* was unresolved. Resolve the source path up until the final
				* path component, then fully resolve the destination.
				*/
				org.apache.hadoop.fs.Path source = resolveIntermediate(absSrc);
				new _FSLinkResolver_965(source, options).resolve(this, absDst);
			}
		}

		private sealed class _FSLinkResolver_965 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_965(org.apache.hadoop.fs.Path source, org.apache.hadoop.fs.Options.Rename
				[] options)
			{
				this.source = source;
				this.options = options;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.rename(source, p, options);
				return null;
			}

			private readonly org.apache.hadoop.fs.Path source;

			private readonly org.apache.hadoop.fs.Options.Rename[] options;
		}

		/// <summary>Set permission of a path.</summary>
		/// <param name="f"/>
		/// <param name="permission">- the new absolute permission (umask is not applied)</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code>
		/// is not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			new _FSLinkResolver_997(permission).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_997 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_997(org.apache.hadoop.fs.permission.FsPermission permission
				)
			{
				this.permission = permission;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.setPermission(p, permission);
				return null;
			}

			private readonly org.apache.hadoop.fs.permission.FsPermission permission;
		}

		/// <summary>Set owner of a path (i.e.</summary>
		/// <remarks>
		/// Set owner of a path (i.e. a file or a directory). The parameters username
		/// and groupname cannot both be null.
		/// </remarks>
		/// <param name="f">The path</param>
		/// <param name="username">If it is null, the original username remains unchanged.</param>
		/// <param name="groupname">If it is null, the original groupname remains unchanged.</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException">
		/// If <code>username</code> or
		/// <code>groupname</code> is invalid.
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			if ((username == null) && (groupname == null))
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("username and groupname cannot both be null"
					);
			}
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			new _FSLinkResolver_1040(username, groupname).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1040 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_1040(string username, string groupname)
			{
				this.username = username;
				this.groupname = groupname;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.setOwner(p, username, groupname);
				return null;
			}

			private readonly string username;

			private readonly string groupname;
		}

		/// <summary>Set access time of a file.</summary>
		/// <param name="f">The path</param>
		/// <param name="mtime">
		/// Set the modification time of this file.
		/// The number of milliseconds since epoch (Jan 1, 1970).
		/// A value of -1 means that this call should not set modification time.
		/// </param>
		/// <param name="atime">
		/// Set the access time of this file.
		/// The number of milliseconds since Jan 1, 1970.
		/// A value of -1 means that this call should not set access time.
		/// </param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			new _FSLinkResolver_1076(mtime, atime).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1076 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_1076(long mtime, long atime)
			{
				this.mtime = mtime;
				this.atime = atime;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.setTimes(p, mtime, atime);
				return null;
			}

			private readonly long mtime;

			private readonly long atime;
		}

		/// <summary>Get the checksum of a file.</summary>
		/// <param name="f">file path</param>
		/// <returns>
		/// The file checksum.  The default return value is null,
		/// which indicates that no checksum algorithm is implemented
		/// in the corresponding FileSystem.
		/// </returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		public virtual org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1109().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1109 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FileChecksum
			>
		{
			public _FSLinkResolver_1109()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FileChecksum next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getFileChecksum(p);
			}
		}

		/// <summary>Set the verify checksum flag for the  file system denoted by the path.</summary>
		/// <remarks>
		/// Set the verify checksum flag for the  file system denoted by the path.
		/// This is only applicable if the
		/// corresponding FileSystem supports checksum. By default doesn't do anything.
		/// </remarks>
		/// <param name="verifyChecksum"/>
		/// <param name="f">set the verifyChecksum for the Filesystem containing this path</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void setVerifyChecksum(bool verifyChecksum, org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.Path absF = resolve(fixRelativePart(f));
			getFSofPath(absF).setVerifyChecksum(verifyChecksum);
		}

		/// <summary>Return a file status object that represents the path.</summary>
		/// <param name="f">The path we want information from</param>
		/// <returns>a FileStatus object</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1165().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1165 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FileStatus
			>
		{
			public _FSLinkResolver_1165()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FileStatus next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getFileStatus(p);
			}
		}

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
		/// <exception cref="UnsupportedFileSystemException">
		/// if file system for <code>path</code>
		/// is not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// see specific implementation
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			org.apache.hadoop.fs.Path absPath = fixRelativePart(path);
			new _FSLinkResolver_1213(mode).resolve(this, absPath);
		}

		private sealed class _FSLinkResolver_1213 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_1213(org.apache.hadoop.fs.permission.FsAction mode)
			{
				this.mode = mode;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.access(p, mode);
				return null;
			}

			private readonly org.apache.hadoop.fs.permission.FsAction mode;
		}

		/// <summary>Return a file status object that represents the path.</summary>
		/// <remarks>
		/// Return a file status object that represents the path. If the path
		/// refers to a symlink then the FileStatus of the symlink is returned.
		/// The behavior is equivalent to #getFileStatus() if the underlying
		/// file system does not support symbolic links.
		/// </remarks>
		/// <param name="f">The path we want information from.</param>
		/// <returns>A FileStatus object</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1241().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1241 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FileStatus
			>
		{
			public _FSLinkResolver_1241()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FileStatus next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				org.apache.hadoop.fs.FileStatus fi = fs.getFileLinkStatus(p);
				if (fi.isSymlink())
				{
					fi.setSymlink(org.apache.hadoop.fs.FSLinkResolver.qualifySymlinkTarget(fs.getUri(
						), p, fi.getSymlink()));
				}
				return fi;
			}
		}

		/// <summary>
		/// Returns the target of the given symbolic link as it was specified
		/// when the link was created.
		/// </summary>
		/// <remarks>
		/// Returns the target of the given symbolic link as it was specified
		/// when the link was created.  Links in the path leading up to the
		/// final path component are resolved transparently.
		/// </remarks>
		/// <param name="f">the path to return the target of</param>
		/// <returns>The un-interpreted target of the symbolic link.</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If path <code>f</code> does not exist
		/// 	</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If the given path does not refer to a symlink
		/// or an I/O error occurred
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path 
			f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1273().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1273 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.Path
			>
		{
			public _FSLinkResolver_1273()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.Path next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				org.apache.hadoop.fs.FileStatus fi = fs.getFileLinkStatus(p);
				return fi.getSymlink();
			}
		}

		/// <summary>Return blockLocation of the given file for the given offset and len.</summary>
		/// <remarks>
		/// Return blockLocation of the given file for the given offset and len.
		/// For a nonexistent file or regions, null will be returned.
		/// This call is most helpful with DFS, where it returns
		/// hostnames of machines that contain the given file.
		/// </remarks>
		/// <param name="f">- get blocklocations of this file</param>
		/// <param name="start">position (byte offset)</param>
		/// <param name="len">(in bytes)</param>
		/// <returns>block locations for given file at specified offset of len</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// RuntimeExceptions:
		/// </exception>
		/// <exception cref="InvalidPathException">If path <code>f</code> is invalid</exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1317(start, len).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1317 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.BlockLocation
			[]>
		{
			public _FSLinkResolver_1317(long start, long len)
			{
				this.start = start;
				this.len = len;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.BlockLocation[] next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getFileBlockLocations(p, start, len);
			}

			private readonly long start;

			private readonly long len;
		}

		/// <summary>
		/// Returns a status object describing the use and capacity of the
		/// file system denoted by the Parh argument p.
		/// </summary>
		/// <remarks>
		/// Returns a status object describing the use and capacity of the
		/// file system denoted by the Parh argument p.
		/// If the file system has multiple partitions, the
		/// use and capacity of the partition pointed to by the specified
		/// path is reflected.
		/// </remarks>
		/// <param name="f">
		/// Path for which status should be obtained. null means the
		/// root partition of the default file system.
		/// </param>
		/// <returns>a FsStatus object</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.FsStatus getFsStatus(org.apache.hadoop.fs.Path
			 f)
		{
			if (f == null)
			{
				return defaultFS.getFsStatus();
			}
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1356().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1356 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FsStatus
			>
		{
			public _FSLinkResolver_1356()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FsStatus next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getFsStatus(p);
			}
		}

		/// <summary>Creates a symbolic link to an existing file.</summary>
		/// <remarks>
		/// Creates a symbolic link to an existing file. An exception is thrown if
		/// the symlink exits, the user does not have permission to create symlink,
		/// or the underlying file system does not support symlinks.
		/// Symlink permissions are ignored, access to a symlink is determined by
		/// the permissions of the symlink target.
		/// Symlinks in paths leading up to the final path component are resolved
		/// transparently. If the final path component refers to a symlink some
		/// functions operate on the symlink itself, these are:
		/// - delete(f) and deleteOnExit(f) - Deletes the symlink.
		/// - rename(src, dst) - If src refers to a symlink, the symlink is
		/// renamed. If dst refers to a symlink, the symlink is over-written.
		/// - getLinkTarget(f) - Returns the target of the symlink.
		/// - getFileLinkStatus(f) - Returns a FileStatus object describing
		/// the symlink.
		/// Some functions, create() and mkdir(), expect the final path component
		/// does not exist. If they are given a path that refers to a symlink that
		/// does exist they behave as if the path referred to an existing file or
		/// directory. All other functions fully resolve, ie follow, the symlink.
		/// These are: open, setReplication, setOwner, setTimes, setWorkingDirectory,
		/// setPermission, getFileChecksum, setVerifyChecksum, getFileBlockLocations,
		/// getFsStatus, getFileStatus, exists, and listStatus.
		/// Symlink targets are stored as given to createSymlink, assuming the
		/// underlying file system is capable of storing a fully qualified URI.
		/// Dangling symlinks are permitted. FileContext supports four types of
		/// symlink targets, and resolves them as follows
		/// <pre>
		/// Given a path referring to a symlink of form:
		/// <---X--->
		/// fs://host/A/B/link
		/// <-----Y----->
		/// In this path X is the scheme and authority that identify the file system,
		/// and Y is the path leading up to the final path component "link". If Y is
		/// a symlink  itself then let Y' be the target of Y and X' be the scheme and
		/// authority of Y'. Symlink targets may:
		/// 1. Fully qualified URIs
		/// fs://hostX/A/B/file  Resolved according to the target file system.
		/// 2. Partially qualified URIs (eg scheme but no host)
		/// fs:///A/B/file  Resolved according to the target file system. Eg resolving
		/// a symlink to hdfs:///A results in an exception because
		/// HDFS URIs must be fully qualified, while a symlink to
		/// file:///A will not since Hadoop's local file systems
		/// require partially qualified URIs.
		/// 3. Relative paths
		/// path  Resolves to [Y'][path]. Eg if Y resolves to hdfs://host/A and path
		/// is "../B/file" then [Y'][path] is hdfs://host/B/file
		/// 4. Absolute paths
		/// path  Resolves to [X'][path]. Eg if Y resolves hdfs://host/A/B and path
		/// is "/file" then [X][path] is hdfs://host/file
		/// </pre>
		/// </remarks>
		/// <param name="target">the target of the symbolic link</param>
		/// <param name="link">the path to be created that points to target</param>
		/// <param name="createParent">
		/// if true then missing parent dirs are created if
		/// false then parent must exist
		/// </param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="FileAlreadyExistsException">If file <code>linkcode&gt; already exists
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>target</code> does not exist
		/// 	</exception>
		/// <exception cref="ParentNotDirectoryException">
		/// If parent of <code>link</code> is not a
		/// directory.
		/// </exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for
		/// <code>target</code> or <code>link</code> is not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			if (!org.apache.hadoop.fs.FileSystem.areSymlinksEnabled())
			{
				throw new System.NotSupportedException("Symlinks not supported");
			}
			org.apache.hadoop.fs.Path nonRelLink = fixRelativePart(link);
			new _FSLinkResolver_1454(target, createParent).resolve(this, nonRelLink);
		}

		private sealed class _FSLinkResolver_1454 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_1454(org.apache.hadoop.fs.Path target, bool createParent)
			{
				this.target = target;
				this.createParent = createParent;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.createSymlink(target, p, createParent);
				return null;
			}

			private readonly org.apache.hadoop.fs.Path target;

			private readonly bool createParent;
		}

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">is the path</param>
		/// <returns>
		/// an iterator that traverses statuses of the files/directories
		/// in the given path
		/// </returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatus(org.apache.hadoop.fs.Path f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1489().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1489 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.RemoteIterator
			<org.apache.hadoop.fs.FileStatus>>
		{
			public _FSLinkResolver_1489()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
				> next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path p)
			{
				return fs.listStatusIterator(p);
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			return new _FSLinkResolver_1507().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1507 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.RemoteIterator
			<org.apache.hadoop.fs.Path>>
		{
			public _FSLinkResolver_1507()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.Path> next
				(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path p)
			{
				return fs.listCorruptFileBlocks(p);
			}
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
		/// If any IO exception (for example the input directory gets deleted while
		/// listing is being executed), next() or hasNext() of the returned iterator
		/// may throw a RuntimeException with the io exception as the cause.
		/// </returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			return new _FSLinkResolver_1549().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_1549 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.RemoteIterator
			<org.apache.hadoop.fs.LocatedFileStatus>>
		{
			public _FSLinkResolver_1549()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
				> next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path p)
			{
				return fs.listLocatedStatus(p);
			}
		}

		/// <summary>Mark a path to be deleted on JVM shutdown.</summary>
		/// <param name="f">the existing path to delete.</param>
		/// <returns>true if deleteOnExit is successful, otherwise false.</returns>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
		/// 	</exception>
		/// <exception cref="UnsupportedFileSystemException">
		/// If file system for <code>f</code> is
		/// not supported
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// If an I/O error occurred
		/// Exceptions applicable to file systems accessed over RPC:
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
		/// If server implementation throws
		/// undeclared exception to RPC server
		/// </exception>
		public virtual bool deleteOnExit(org.apache.hadoop.fs.Path f)
		{
			if (!this.util().exists(f))
			{
				return false;
			}
			lock (DELETE_ON_EXIT)
			{
				if (DELETE_ON_EXIT.isEmpty())
				{
					org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(FINALIZER, SHUTDOWN_HOOK_PRIORITY
						);
				}
				System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path> set = DELETE_ON_EXIT
					[this];
				if (set == null)
				{
					set = new java.util.TreeSet<org.apache.hadoop.fs.Path>();
					DELETE_ON_EXIT[this] = set;
				}
				set.add(f);
			}
			return true;
		}

		private readonly org.apache.hadoop.fs.FileContext.Util util;

		public virtual org.apache.hadoop.fs.FileContext.Util util()
		{
			return util;
		}

		/// <summary>Utility/library methods built over the basic FileContext methods.</summary>
		/// <remarks>
		/// Utility/library methods built over the basic FileContext methods.
		/// Since this are library functions, the oprtation are not atomic
		/// and some of them may partially complete if other threads are making
		/// changes to the same part of the name space.
		/// </remarks>
		public class Util
		{
			/// <summary>
			/// Does the file exist?
			/// Note: Avoid using this method if you already have FileStatus in hand.
			/// </summary>
			/// <remarks>
			/// Does the file exist?
			/// Note: Avoid using this method if you already have FileStatus in hand.
			/// Instead reuse the FileStatus
			/// </remarks>
			/// <param name="f">the  file or dir to be checked</param>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for <code>f</code> is
			/// not supported
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual bool exists(org.apache.hadoop.fs.Path f)
			{
				try
				{
					org.apache.hadoop.fs.FileStatus fs = this._enclosing.getFileStatus(f);
					System.Diagnostics.Debug.Assert(fs != null);
					return true;
				}
				catch (java.io.FileNotFoundException)
				{
					return false;
				}
			}

			/// <summary>
			/// Return the
			/// <see cref="ContentSummary"/>
			/// of path f.
			/// </summary>
			/// <param name="f">path</param>
			/// <returns>
			/// the
			/// <see cref="ContentSummary"/>
			/// of path f.
			/// </returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for
			/// <code>f</code> is not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path
				 f)
			{
				org.apache.hadoop.fs.FileStatus status = this._enclosing.getFileStatus(f);
				if (status.isFile())
				{
					long length = status.getLen();
					return new org.apache.hadoop.fs.ContentSummary.Builder().length(length).fileCount
						(1).directoryCount(0).spaceConsumed(length).build();
				}
				long[] summary = new long[] { 0, 0, 1 };
				org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> statusIterator
					 = this._enclosing.listStatus(f);
				while (statusIterator.hasNext())
				{
					org.apache.hadoop.fs.FileStatus s = statusIterator.next();
					long length = s.getLen();
					org.apache.hadoop.fs.ContentSummary c = s.isDirectory() ? this.getContentSummary(
						s.getPath()) : new org.apache.hadoop.fs.ContentSummary.Builder().length(length).
						fileCount(1).directoryCount(0).spaceConsumed(length).build();
					summary[0] += c.getLength();
					summary[1] += c.getFileCount();
					summary[2] += c.getDirectoryCount();
				}
				return new org.apache.hadoop.fs.ContentSummary.Builder().length(summary[0]).fileCount
					(summary[1]).directoryCount(summary[2]).spaceConsumed(summary[0]).build();
			}

			/// <summary>
			/// See
			/// <see cref="listStatus(Path[], PathFilter)"/>
			/// </summary>
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				[] files)
			{
				return this.listStatus(files, org.apache.hadoop.fs.FileContext.DEFAULT_FILTER);
			}

			/// <summary>
			/// Filter files/directories in the given path using the user-supplied path
			/// filter.
			/// </summary>
			/// <param name="f">is the path name</param>
			/// <param name="filter">is the user-supplied path filter</param>
			/// <returns>
			/// an array of FileStatus objects for the files under the given path
			/// after applying the filter
			/// </returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for
			/// <code>pathPattern</code> is not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.PathFilter filter)
			{
				System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> results = new System.Collections.Generic.List
					<org.apache.hadoop.fs.FileStatus>();
				this.listStatus(results, f, filter);
				return Sharpen.Collections.ToArray(results, new org.apache.hadoop.fs.FileStatus[results
					.Count]);
			}

			/// <summary>
			/// Filter files/directories in the given list of paths using user-supplied
			/// path filter.
			/// </summary>
			/// <param name="files">is a list of paths</param>
			/// <param name="filter">is the filter</param>
			/// <returns>
			/// a list of statuses for the files under the given paths after
			/// applying the filter
			/// </returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="java.io.FileNotFoundException">
			/// If a file in <code>files</code> does not
			/// exist
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				[] files, org.apache.hadoop.fs.PathFilter filter)
			{
				System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> results = new System.Collections.Generic.List
					<org.apache.hadoop.fs.FileStatus>();
				for (int i = 0; i < files.Length; i++)
				{
					this.listStatus(results, files[i], filter);
				}
				return Sharpen.Collections.ToArray(results, new org.apache.hadoop.fs.FileStatus[results
					.Count]);
			}

			/*
			* Filter files/directories in the given path using the user-supplied path
			* filter. Results are added to the given array <code>results</code>.
			*/
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			private void listStatus(System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus
				> results, org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter filter)
			{
				org.apache.hadoop.fs.FileStatus[] listing = this.listStatus(f);
				if (listing != null)
				{
					for (int i = 0; i < listing.Length; i++)
					{
						if (filter.accept(listing[i].getPath()))
						{
							results.add(listing[i]);
						}
					}
				}
			}

			/// <summary>
			/// List the statuses of the files/directories in the given path
			/// if the path is a directory.
			/// </summary>
			/// <param name="f">is the path</param>
			/// <returns>
			/// an array that contains statuses of the files/directories
			/// in the given path
			/// </returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for <code>f</code> is
			/// not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f)
			{
				org.apache.hadoop.fs.Path absF = this._enclosing.fixRelativePart(f);
				return new _FSLinkResolver_1794().resolve(this._enclosing, absF);
			}

			private sealed class _FSLinkResolver_1794 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FileStatus
				[]>
			{
				public _FSLinkResolver_1794()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
				public override org.apache.hadoop.fs.FileStatus[] next(org.apache.hadoop.fs.AbstractFileSystem
					 fs, org.apache.hadoop.fs.Path p)
				{
					return fs.listStatus(p);
				}
			}

			/// <summary>List the statuses and block locations of the files in the given path.</summary>
			/// <remarks>
			/// List the statuses and block locations of the files in the given path.
			/// If the path is a directory,
			/// if recursive is false, returns files in the directory;
			/// if recursive is true, return files in the subtree rooted at the path.
			/// The subtree is traversed in the depth-first order.
			/// If the path is a file, return the file's status and block locations.
			/// Files across symbolic links are also returned.
			/// </remarks>
			/// <param name="f">is the path</param>
			/// <param name="recursive">if the subdirectories need to be traversed recursively</param>
			/// <returns>
			/// an iterator that traverses statuses of the files
			/// If any IO exception (for example a sub-directory gets deleted while
			/// listing is being executed), next() or hasNext() of the returned iterator
			/// may throw a RuntimeException with the IO exception as the cause.
			/// </returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="java.io.FileNotFoundException">If <code>f</code> does not exist</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for <code>f</code>
			/// is not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
				> listFiles(org.apache.hadoop.fs.Path f, bool recursive)
			{
				return new _RemoteIterator_1837(this, f, recursive);
			}

			private sealed class _RemoteIterator_1837 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
				>
			{
				public _RemoteIterator_1837(Util _enclosing, org.apache.hadoop.fs.Path f, bool recursive
					)
				{
					this._enclosing = _enclosing;
					this.f = f;
					this.recursive = recursive;
					this.itors = new java.util.Stack<org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
						>>();
					this.curItor = this._enclosing._enclosing.listLocatedStatus(f);
				}

				private java.util.Stack<org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
					>> itors;

				internal org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
					> curItor;

				internal org.apache.hadoop.fs.LocatedFileStatus curFile;

				/// <summary>Returns <tt>true</tt> if the iterator has more files.</summary>
				/// <returns><tt>true</tt> if the iterator has more files.</returns>
				/// <exception cref="org.apache.hadoop.security.AccessControlException">
				/// if not allowed to access next
				/// file's status or locations
				/// </exception>
				/// <exception cref="java.io.FileNotFoundException">if next file does not exist any more
				/// 	</exception>
				/// <exception cref="UnsupportedFileSystemException">
				/// if next file's
				/// fs is unsupported
				/// </exception>
				/// <exception cref="System.IO.IOException">
				/// for all other IO errors
				/// for example, NameNode is not avaialbe or
				/// NameNode throws IOException due to an error
				/// while getting the status or block locations
				/// </exception>
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
				/// If it is a symlink, resolve the symlink first and then process it
				/// depending on if it is a file or directory.
				/// </remarks>
				/// <param name="stat">input status</param>
				/// <exception cref="org.apache.hadoop.security.AccessControlException">if access is denied
				/// 	</exception>
				/// <exception cref="java.io.FileNotFoundException">if file is not found</exception>
				/// <exception cref="UnsupportedFileSystemException">if fs is not supported</exception>
				/// <exception cref="System.IO.IOException">for all other IO errors</exception>
				private void handleFileStat(org.apache.hadoop.fs.LocatedFileStatus stat)
				{
					if (stat.isFile())
					{
						// file
						this.curFile = stat;
					}
					else
					{
						if (stat.isSymlink())
						{
							// symbolic link
							// resolve symbolic link
							org.apache.hadoop.fs.FileStatus symstat = this._enclosing._enclosing.getFileStatus
								(stat.getSymlink());
							if (symstat.isFile() || (recursive && symstat.isDirectory()))
							{
								this.itors.push(this.curItor);
								this.curItor = this._enclosing._enclosing.listLocatedStatus(stat.getPath());
							}
						}
						else
						{
							if (recursive)
							{
								// directory
								this.itors.push(this.curItor);
								this.curItor = this._enclosing._enclosing.listLocatedStatus(stat.getPath());
							}
						}
					}
				}

				/// <summary>Returns the next file's status with its block locations</summary>
				/// <exception cref="org.apache.hadoop.security.AccessControlException">
				/// if not allowed to access next
				/// file's status or locations
				/// </exception>
				/// <exception cref="java.io.FileNotFoundException">if next file does not exist any more
				/// 	</exception>
				/// <exception cref="UnsupportedFileSystemException">
				/// if next file's
				/// fs is unsupported
				/// </exception>
				/// <exception cref="System.IO.IOException">
				/// for all other IO errors
				/// for example, NameNode is not avaialbe or
				/// NameNode throws IOException due to an error
				/// while getting the status or block locations
				/// </exception>
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

				private readonly Util _enclosing;

				private readonly org.apache.hadoop.fs.Path f;

				private readonly bool recursive;
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
			/// <tt>{<i>a...b</i>}</tt>. Note: character <tt><i>a</i></tt> must be
			/// lexicographically less than or equal to character <tt><i>b</i></tt>.
			/// <p>
			/// <dt> <tt> [^<i>a</i>] </tt>
			/// <dd> Matches a single char that is not from character set or range
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
			/// <dd> Matches a string from string set <tt>{<i>ab, cde, cfh</i>}</tt>
			/// </dl>
			/// </dd>
			/// </dl>
			/// </remarks>
			/// <param name="pathPattern">a regular expression specifying a pth pattern</param>
			/// <returns>an array of paths that match the path pattern</returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for
			/// <code>pathPattern</code> is not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
				 pathPattern)
			{
				return new org.apache.hadoop.fs.Globber(this._enclosing, pathPattern, org.apache.hadoop.fs.FileContext
					.DEFAULT_FILTER).glob();
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
			/// <param name="pathPattern">regular expression specifying the path pattern</param>
			/// <param name="filter">user-supplied path filter</param>
			/// <returns>an array of FileStatus objects</returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for
			/// <code>pathPattern</code> is not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// </exception>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
				 pathPattern, org.apache.hadoop.fs.PathFilter filter)
			{
				return new org.apache.hadoop.fs.Globber(this._enclosing, pathPattern, filter).glob
					();
			}

			/// <summary>Copy file from src to dest.</summary>
			/// <remarks>
			/// Copy file from src to dest. See
			/// <see cref="copy(Path, Path, bool, bool)"/>
			/// </remarks>
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool copy(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
				return this.copy(src, dst, false, false);
			}

			/// <summary>Copy from src to dst, optionally deleting src and overwriting dst.</summary>
			/// <param name="src"/>
			/// <param name="dst"/>
			/// <param name="deleteSource">- delete src if true</param>
			/// <param name="overwrite">
			/// overwrite dst if true; throw IOException if dst exists
			/// and overwrite is false.
			/// </param>
			/// <returns>true if copy is successful</returns>
			/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied
			/// 	</exception>
			/// <exception cref="FileAlreadyExistsException">If <code>dst</code> already exists</exception>
			/// <exception cref="java.io.FileNotFoundException">If <code>src</code> does not exist
			/// 	</exception>
			/// <exception cref="ParentNotDirectoryException">
			/// If parent of <code>dst</code> is not
			/// a directory
			/// </exception>
			/// <exception cref="UnsupportedFileSystemException">
			/// If file system for
			/// <code>src</code> or <code>dst</code> is not supported
			/// </exception>
			/// <exception cref="System.IO.IOException">
			/// If an I/O error occurred
			/// Exceptions applicable to file systems accessed over RPC:
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcClientException">If an exception occurred in the RPC client
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.RpcServerException">If an exception occurred in the RPC server
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.ipc.UnexpectedServerException">
			/// If server implementation throws
			/// undeclared exception to RPC server
			/// RuntimeExceptions:
			/// </exception>
			/// <exception cref="InvalidPathException">If path <code>dst</code> is invalid</exception>
			/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
			/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			public virtual bool copy(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst, bool deleteSource, bool overwrite)
			{
				src.checkNotSchemeWithRelative();
				dst.checkNotSchemeWithRelative();
				org.apache.hadoop.fs.Path qSrc = this._enclosing.makeQualified(src);
				org.apache.hadoop.fs.Path qDst = this._enclosing.makeQualified(dst);
				this._enclosing.checkDest(qSrc.getName(), qDst, overwrite);
				org.apache.hadoop.fs.FileStatus fs = this._enclosing.getFileStatus(qSrc);
				if (fs.isDirectory())
				{
					org.apache.hadoop.fs.FileContext.checkDependencies(qSrc, qDst);
					this._enclosing.mkdir(qDst, org.apache.hadoop.fs.permission.FsPermission.getDirDefault
						(), true);
					org.apache.hadoop.fs.FileStatus[] contents = this.listStatus(qSrc);
					foreach (org.apache.hadoop.fs.FileStatus content in contents)
					{
						this.copy(this._enclosing.makeQualified(content.getPath()), this._enclosing.makeQualified
							(new org.apache.hadoop.fs.Path(qDst, content.getPath().getName())), deleteSource
							, overwrite);
					}
				}
				else
				{
					java.io.InputStream @in = null;
					java.io.OutputStream @out = null;
					try
					{
						@in = this._enclosing.open(qSrc);
						java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag = overwrite ? java.util.EnumSet
							.of(org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.OVERWRITE
							) : java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.CREATE);
						@out = this._enclosing.create(qDst, createFlag);
						org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, this._enclosing.conf, true);
					}
					finally
					{
						org.apache.hadoop.io.IOUtils.closeStream(@out);
						org.apache.hadoop.io.IOUtils.closeStream(@in);
					}
				}
				if (deleteSource)
				{
					return this._enclosing.delete(qSrc, true);
				}
				else
				{
					return true;
				}
			}

			internal Util(FileContext _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FileContext _enclosing;
		}

		/// <summary>
		/// Check if copying srcName to dst would overwrite an existing
		/// file or directory.
		/// </summary>
		/// <param name="srcName">File or directory to be copied.</param>
		/// <param name="dst">Destination to copy srcName to.</param>
		/// <param name="overwrite">Whether it's ok to overwrite an existing file.</param>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">If access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">
		/// If dst is an existing directory, or dst is an
		/// existing file and the overwrite option is not passed.
		/// </exception>
		private void checkDest(string srcName, org.apache.hadoop.fs.Path dst, bool overwrite
			)
		{
			try
			{
				org.apache.hadoop.fs.FileStatus dstFs = getFileStatus(dst);
				if (dstFs.isDirectory())
				{
					if (null == srcName)
					{
						throw new System.IO.IOException("Target " + dst + " is a directory");
					}
					// Recurse to check if dst/srcName exists.
					checkDest(null, new org.apache.hadoop.fs.Path(dst, srcName), overwrite);
				}
				else
				{
					if (!overwrite)
					{
						throw new System.IO.IOException("Target " + new org.apache.hadoop.fs.Path(dst, srcName
							) + " already exists");
					}
				}
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// dst does not exist - OK to copy.
		//
		// If the destination is a subdirectory of the source, then
		// generate exception
		//
		/// <exception cref="System.IO.IOException"/>
		private static void checkDependencies(org.apache.hadoop.fs.Path qualSrc, org.apache.hadoop.fs.Path
			 qualDst)
		{
			if (isSameFS(qualSrc, qualDst))
			{
				string srcq = qualSrc.ToString() + org.apache.hadoop.fs.Path.SEPARATOR;
				string dstq = qualDst.ToString() + org.apache.hadoop.fs.Path.SEPARATOR;
				if (dstq.StartsWith(srcq))
				{
					if (srcq.Length == dstq.Length)
					{
						throw new System.IO.IOException("Cannot copy " + qualSrc + " to itself.");
					}
					else
					{
						throw new System.IO.IOException("Cannot copy " + qualSrc + " to its subdirectory "
							 + qualDst);
					}
				}
			}
		}

		/// <summary>Are qualSrc and qualDst of the same file system?</summary>
		/// <param name="qualPath1">- fully qualified path</param>
		/// <param name="qualPath2">- fully qualified path</param>
		/// <returns/>
		private static bool isSameFS(org.apache.hadoop.fs.Path qualPath1, org.apache.hadoop.fs.Path
			 qualPath2)
		{
			java.net.URI srcUri = qualPath1.toUri();
			java.net.URI dstUri = qualPath2.toUri();
			return (srcUri.getScheme().Equals(dstUri.getScheme()) && !(srcUri.getAuthority() 
				!= null && dstUri.getAuthority() != null && srcUri.getAuthority().Equals(dstUri.
				getAuthority())));
		}

		/// <summary>Deletes all the paths in deleteOnExit on JVM shutdown.</summary>
		internal class FileContextFinalizer : java.lang.Runnable
		{
			public virtual void run()
			{
				lock (this)
				{
					processDeleteOnExit();
				}
			}
		}

		/// <summary>Resolves all symbolic links in the specified path.</summary>
		/// <remarks>
		/// Resolves all symbolic links in the specified path.
		/// Returns the new path object.
		/// </remarks>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Path resolve(org.apache.hadoop.fs.Path
			 f)
		{
			return new _FSLinkResolver_2189().resolve(this, f);
		}

		private sealed class _FSLinkResolver_2189 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.Path
			>
		{
			public _FSLinkResolver_2189()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.Path next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.resolvePath(p);
			}
		}

		/// <summary>
		/// Resolves all symbolic links in the specified path leading up
		/// to, but not including the final path component.
		/// </summary>
		/// <param name="f">path to resolve</param>
		/// <returns>the new path object.</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Path resolveIntermediate(org.apache.hadoop.fs.Path
			 f)
		{
			return new _FSLinkResolver_2205().resolve(this, f).getPath();
		}

		private sealed class _FSLinkResolver_2205 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.FileStatus
			>
		{
			public _FSLinkResolver_2205()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override org.apache.hadoop.fs.FileStatus next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getFileLinkStatus(p);
			}
		}

		/// <summary>Returns the list of AbstractFileSystems accessed in the path.</summary>
		/// <remarks>
		/// Returns the list of AbstractFileSystems accessed in the path. The list may
		/// contain more than one AbstractFileSystems objects in case of symlinks.
		/// </remarks>
		/// <param name="f">Path which needs to be resolved</param>
		/// <returns>List of AbstractFileSystems accessed in the path</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual System.Collections.Generic.ICollection<org.apache.hadoop.fs.AbstractFileSystem
			> resolveAbstractFileSystems(org.apache.hadoop.fs.Path f)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(f);
			java.util.HashSet<org.apache.hadoop.fs.AbstractFileSystem> result = new java.util.HashSet
				<org.apache.hadoop.fs.AbstractFileSystem>();
			new _FSLinkResolver_2228(result).resolve(this, absF);
			return result;
		}

		private sealed class _FSLinkResolver_2228 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2228(java.util.HashSet<org.apache.hadoop.fs.AbstractFileSystem
				> result)
			{
				this.result = result;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				result.add(fs);
				fs.getFileStatus(p);
				return null;
			}

			private readonly java.util.HashSet<org.apache.hadoop.fs.AbstractFileSystem> result;
		}

		/// <summary>Get the statistics for a particular file system</summary>
		/// <param name="uri">
		/// the uri to lookup the statistics. Only scheme and authority part
		/// of the uri are used as the key to store and lookup.
		/// </param>
		/// <returns>a statistics object</returns>
		public static org.apache.hadoop.fs.FileSystem.Statistics getStatistics(java.net.URI
			 uri)
		{
			return org.apache.hadoop.fs.AbstractFileSystem.getStatistics(uri);
		}

		/// <summary>
		/// Clears all the statistics stored in AbstractFileSystem, for all the file
		/// systems.
		/// </summary>
		public static void clearStatistics()
		{
			org.apache.hadoop.fs.AbstractFileSystem.clearStatistics();
		}

		/// <summary>Prints the statistics to standard output.</summary>
		/// <remarks>
		/// Prints the statistics to standard output. File System is identified by the
		/// scheme and authority.
		/// </remarks>
		public static void printStatistics()
		{
			org.apache.hadoop.fs.AbstractFileSystem.printStatistics();
		}

		/// <returns>
		/// Map of uri and statistics for each filesystem instantiated. The uri
		/// consists of scheme and authority for the filesystem.
		/// </returns>
		public static System.Collections.Generic.IDictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
			> getAllStatistics()
		{
			return org.apache.hadoop.fs.AbstractFileSystem.getAllStatistics();
		}

		/// <summary>
		/// Get delegation tokens for the file systems accessed for a given
		/// path.
		/// </summary>
		/// <param name="p">Path for which delegations tokens are requested.</param>
		/// <param name="renewer">the account name that is allowed to renew the token.</param>
		/// <returns>List of delegation tokens.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> getDelegationTokens(org.apache.hadoop.fs.Path p, string renewer)
		{
			System.Collections.Generic.ICollection<org.apache.hadoop.fs.AbstractFileSystem> afsSet
				 = resolveAbstractFileSystems(p);
			System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
				tokenList = new System.Collections.Generic.List<org.apache.hadoop.security.token.Token
				<object>>();
			foreach (org.apache.hadoop.fs.AbstractFileSystem afs in afsSet)
			{
				System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
					afsTokens = afs.getDelegationTokens(renewer);
				Sharpen.Collections.AddAll(tokenList, afsTokens);
			}
			return tokenList;
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2310(aclSpec).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2310 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2310(System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec)
			{
				this.aclSpec = aclSpec;
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.modifyAclEntries(p, aclSpec);
				return null;
			}

			private readonly System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec;
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2331(aclSpec).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2331 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2331(System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec)
			{
				this.aclSpec = aclSpec;
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.removeAclEntries(p, aclSpec);
				return null;
			}

			private readonly System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec;
		}

		/// <summary>Removes all default ACL entries from files and directories.</summary>
		/// <param name="path">Path to modify</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		public virtual void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2350().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2350 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2350()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.removeDefaultAcl(p);
				return null;
			}
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2370().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2370 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2370()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.removeAcl(p);
				return null;
			}
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2392(aclSpec).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2392 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2392(System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec)
			{
				this.aclSpec = aclSpec;
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.setAcl(p, aclSpec);
				return null;
			}

			private readonly System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec;
		}

		/// <summary>Gets the ACLs of files and directories.</summary>
		/// <param name="path">Path to get</param>
		/// <returns>RemoteIterator<AclStatus> which returns each AclStatus</returns>
		/// <exception cref="System.IO.IOException">if an ACL could not be read</exception>
		public virtual org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			return new _FSLinkResolver_2411().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2411 : org.apache.hadoop.fs.FSLinkResolver<org.apache.hadoop.fs.permission.AclStatus
			>
		{
			public _FSLinkResolver_2411()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.permission.AclStatus next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getAclStatus(p);
			}
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2454(name, value, flag).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2454 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2454(string name, byte[] value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag
				> flag)
			{
				this.name = name;
				this.value = value;
				this.flag = flag;
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.setXAttr(p, name, value, flag);
				return null;
			}

			private readonly string name;

			private readonly byte[] value;

			private readonly java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag;
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			return new _FSLinkResolver_2478(name).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2478 : org.apache.hadoop.fs.FSLinkResolver<byte
			[]>
		{
			public _FSLinkResolver_2478(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public override byte[] next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				return fs.getXAttr(p, name);
			}

			private readonly string name;
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			return new _FSLinkResolver_2500().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2500 : org.apache.hadoop.fs.FSLinkResolver<System.Collections.Generic.IDictionary
			<string, byte[]>>
		{
			public _FSLinkResolver_2500()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IDictionary<string, byte[]> next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getXAttrs(p);
			}
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			return new _FSLinkResolver_2524(names).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2524 : org.apache.hadoop.fs.FSLinkResolver<System.Collections.Generic.IDictionary
			<string, byte[]>>
		{
			public _FSLinkResolver_2524(System.Collections.Generic.IList<string> names)
			{
				this.names = names;
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IDictionary<string, byte[]> next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.getXAttrs(p, names);
			}

			private readonly System.Collections.Generic.IList<string> names;
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			new _FSLinkResolver_2546(name).resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2546 : org.apache.hadoop.fs.FSLinkResolver<java.lang.Void
			>
		{
			public _FSLinkResolver_2546(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Void next(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
				 p)
			{
				fs.removeXAttr(p, name);
				return null;
			}

			private readonly string name;
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
			org.apache.hadoop.fs.Path absF = fixRelativePart(path);
			return new _FSLinkResolver_2569().resolve(this, absF);
		}

		private sealed class _FSLinkResolver_2569 : org.apache.hadoop.fs.FSLinkResolver<System.Collections.Generic.IList
			<string>>
		{
			public _FSLinkResolver_2569()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IList<string> next(org.apache.hadoop.fs.AbstractFileSystem
				 fs, org.apache.hadoop.fs.Path p)
			{
				return fs.listXAttrs(p);
			}
		}
	}
}

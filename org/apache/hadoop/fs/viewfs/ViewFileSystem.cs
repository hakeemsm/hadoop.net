using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// ViewFileSystem (extends the FileSystem interface) implements a client-side
	/// mount table.
	/// </summary>
	/// <remarks>
	/// ViewFileSystem (extends the FileSystem interface) implements a client-side
	/// mount table. Its spec and implementation is identical to
	/// <see cref="ViewFs"/>
	/// .
	/// </remarks>
	public class ViewFileSystem : org.apache.hadoop.fs.FileSystem
	{
		private static readonly org.apache.hadoop.fs.Path ROOT_PATH = new org.apache.hadoop.fs.Path
			(org.apache.hadoop.fs.Path.SEPARATOR);

		/*Evolving for a release,to be changed to Stable */
		internal static org.apache.hadoop.security.AccessControlException readOnlyMountTable
			(string operation, string p)
		{
			return new org.apache.hadoop.security.AccessControlException("InternalDir of ViewFileSystem is readonly; operation="
				 + operation + "Path=" + p);
		}

		internal static org.apache.hadoop.security.AccessControlException readOnlyMountTable
			(string operation, org.apache.hadoop.fs.Path p)
		{
			return readOnlyMountTable(operation, p.ToString());
		}

		public class MountPoint
		{
			private org.apache.hadoop.fs.Path src;

			private java.net.URI[] targets;

			internal MountPoint(org.apache.hadoop.fs.Path srcPath, java.net.URI[] targetURIs)
			{
				// the src of the mount
				//  target of the mount; Multiple targets imply mergeMount
				src = srcPath;
				targets = targetURIs;
			}

			internal virtual org.apache.hadoop.fs.Path getSrc()
			{
				return src;
			}

			internal virtual java.net.URI[] getTargets()
			{
				return targets;
			}
		}

		internal readonly long creationTime;

		internal readonly org.apache.hadoop.security.UserGroupInformation ugi;

		internal java.net.URI myUri;

		private org.apache.hadoop.fs.Path workingDir;

		internal org.apache.hadoop.conf.Configuration config;

		internal org.apache.hadoop.fs.viewfs.InodeTree<org.apache.hadoop.fs.FileSystem> fsState;

		internal org.apache.hadoop.fs.Path homeDir = null;

		// of the the mount table
		// the user/group of user who created mtable
		// the fs state; ie the mount table
		/// <summary>Make the path Absolute and get the path-part of a pathname.</summary>
		/// <remarks>
		/// Make the path Absolute and get the path-part of a pathname.
		/// Checks that URI matches this file system
		/// and that the path-part is a valid name.
		/// </remarks>
		/// <param name="p">path</param>
		/// <returns>path-part of the Path p</returns>
		private string getUriPath(org.apache.hadoop.fs.Path p)
		{
			checkPath(p);
			return makeAbsolute(p).toUri().getPath();
		}

		private org.apache.hadoop.fs.Path makeAbsolute(org.apache.hadoop.fs.Path f)
		{
			return f.isAbsolute() ? f : new org.apache.hadoop.fs.Path(workingDir, f);
		}

		/// <summary>
		/// This is the  constructor with the signature needed by
		/// <see cref="org.apache.hadoop.fs.FileSystem.createFileSystem(java.net.URI, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// After this constructor is called initialize() is called.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		public ViewFileSystem()
		{
			ugi = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
			creationTime = org.apache.hadoop.util.Time.now();
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>viewfs</code></returns>
		public override string getScheme()
		{
			return "viewfs";
		}

		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="theUri">
		/// a uri whose authority section names the host, port, etc. for
		/// this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public override void initialize(java.net.URI theUri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			base.initialize(theUri, conf);
			setConf(conf);
			config = conf;
			// Now build  client side view (i.e. client side mount table) from config.
			string authority = theUri.getAuthority();
			try
			{
				myUri = new java.net.URI(org.apache.hadoop.fs.FsConstants.VIEWFS_SCHEME, authority
					, "/", null, null);
				fsState = new _InodeTree_167(this, conf, authority);
				// return MergeFs.createMergeFs(mergeFsURIList, config);
				workingDir = this.getHomeDirectory();
			}
			catch (java.net.URISyntaxException)
			{
				throw new System.IO.IOException("URISyntax exception: " + theUri);
			}
		}

		private sealed class _InodeTree_167 : org.apache.hadoop.fs.viewfs.InodeTree<org.apache.hadoop.fs.FileSystem
			>
		{
			public _InodeTree_167(ViewFileSystem _enclosing, org.apache.hadoop.conf.Configuration
				 baseArg1, string baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="java.net.URISyntaxException"/>
			/// <exception cref="System.IO.IOException"/>
			protected internal override org.apache.hadoop.fs.FileSystem getTargetFileSystem(java.net.URI
				 uri)
			{
				return new org.apache.hadoop.fs.viewfs.ChRootedFileSystem(uri, this._enclosing.config
					);
			}

			/// <exception cref="java.net.URISyntaxException"/>
			protected internal override org.apache.hadoop.fs.FileSystem getTargetFileSystem(org.apache.hadoop.fs.viewfs.InodeTree.INodeDir
				<org.apache.hadoop.fs.FileSystem> dir)
			{
				return new org.apache.hadoop.fs.viewfs.ViewFileSystem.InternalDirOfViewFs(dir, this
					._enclosing.creationTime, this._enclosing.ugi, this._enclosing.myUri);
			}

			/// <exception cref="java.net.URISyntaxException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			protected internal override org.apache.hadoop.fs.FileSystem getTargetFileSystem(java.net.URI
				[] mergeFsURIList)
			{
				throw new org.apache.hadoop.fs.UnsupportedFileSystemException("mergefs not implemented"
					);
			}

			private readonly ViewFileSystem _enclosing;
		}

		/// <summary>Convenience Constructor for apps to call directly</summary>
		/// <param name="theUri">which must be that of ViewFileSystem</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		internal ViewFileSystem(java.net.URI theUri, org.apache.hadoop.conf.Configuration
			 conf)
			: this()
		{
			initialize(theUri, conf);
		}

		/// <summary>Convenience Constructor for apps to call directly</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public ViewFileSystem(org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, conf)
		{
		}

		/// <exception cref="java.io.FileNotFoundException"/>
		public virtual org.apache.hadoop.fs.Path getTrashCanLocation(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.isInternalDir() ? null : res.targetFileSystem.getHomeDirectory();
		}

		public override java.net.URI getUri()
		{
			return myUri;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path f
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res;
			res = fsState.resolve(getUriPath(f), true);
			if (res.isInternalDir())
			{
				return f;
			}
			return res.targetFileSystem.resolvePath(res.remainingPath);
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			if (homeDir == null)
			{
				string @base = fsState.getHomeDirPrefixValue();
				if (@base == null)
				{
					@base = "/user";
				}
				homeDir = (@base.Equals("/") ? this.makeQualified(new org.apache.hadoop.fs.Path(@base
					 + ugi.getShortUserName())) : this.makeQualified(new org.apache.hadoop.fs.Path(@base
					 + "/" + ugi.getShortUserName())));
			}
			return homeDir;
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return workingDir;
		}

		public override void setWorkingDirectory(org.apache.hadoop.fs.Path new_dir)
		{
			getUriPath(new_dir);
			// this validates the path
			workingDir = makeAbsolute(new_dir);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.append(res.remainingPath, bufferSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res;
			try
			{
				res = fsState.resolve(getUriPath(f), false);
			}
			catch (java.io.FileNotFoundException)
			{
				throw readOnlyMountTable("create", f);
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			return res.targetFileSystem.createNonRecursive(res.remainingPath, permission, flags
				, bufferSize, replication, blockSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res;
			try
			{
				res = fsState.resolve(getUriPath(f), false);
			}
			catch (java.io.FileNotFoundException)
			{
				throw readOnlyMountTable("create", f);
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			return res.targetFileSystem.create(res.remainingPath, permission, overwrite, bufferSize
				, replication, blockSize, progress);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			// If internal dir or target is a mount link (ie remainingPath is Slash)
			if (res.isInternalDir() || res.remainingPath == org.apache.hadoop.fs.viewfs.InodeTree
				.SlashPath)
			{
				throw readOnlyMountTable("delete", f);
			}
			return res.targetFileSystem.delete(res.remainingPath, recursive);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f)
		{
			return delete(f, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus
			 fs, long start, long len)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(fs.getPath()), true);
			return res.targetFileSystem.getFileBlockLocations(new org.apache.hadoop.fs.viewfs.ViewFsFileStatus
				(fs, res.remainingPath), start, len);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.getFileChecksum(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.FileStatus fixFileStatus(org.apache.hadoop.fs.FileStatus
			 orig, org.apache.hadoop.fs.Path qualified)
		{
			// FileStatus#getPath is a fully qualified path relative to the root of
			// target file system.
			// We need to change it to viewfs URI - relative to root of mount table.
			// The implementors of RawLocalFileSystem were trying to be very smart.
			// They implement FileStatus#getOwner lazily -- the object
			// returned is really a RawLocalFileSystem that expect the
			// FileStatus#getPath to be unchanged so that it can get owner when needed.
			// Hence we need to interpose a new ViewFileSystemFileStatus that
			// works around.
			if ("file".Equals(orig.getPath().toUri().getScheme()))
			{
				orig = wrapLocalFileStatus(orig, qualified);
			}
			orig.setPath(qualified);
			return orig;
		}

		private static org.apache.hadoop.fs.FileStatus wrapLocalFileStatus(org.apache.hadoop.fs.FileStatus
			 orig, org.apache.hadoop.fs.Path qualified)
		{
			return orig is org.apache.hadoop.fs.LocatedFileStatus ? new org.apache.hadoop.fs.viewfs.ViewFsLocatedFileStatus
				((org.apache.hadoop.fs.LocatedFileStatus)orig, qualified) : new org.apache.hadoop.fs.viewfs.ViewFsFileStatus
				(orig, qualified);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			org.apache.hadoop.fs.FileStatus status = res.targetFileSystem.getFileStatus(res.remainingPath
				);
			return fixFileStatus(status, this.makeQualified(f));
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.access(res.remainingPath, mode);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			org.apache.hadoop.fs.FileStatus[] statusLst = res.targetFileSystem.listStatus(res
				.remainingPath);
			if (!res.isInternalDir())
			{
				// We need to change the name in the FileStatus as described in
				// {@link #getFileStatus }
				int i = 0;
				foreach (org.apache.hadoop.fs.FileStatus status in statusLst)
				{
					statusLst[i++] = fixFileStatus(status, getChrootedPath(res, status, f));
				}
			}
			return statusLst;
		}

		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter
			 filter)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> statusIter
				 = res.targetFileSystem.listLocatedStatus(res.remainingPath);
			if (res.isInternalDir())
			{
				return statusIter;
			}
			return new _RemoteIterator_422(this, statusIter, res, f);
		}

		private sealed class _RemoteIterator_422 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			>
		{
			public _RemoteIterator_422(ViewFileSystem _enclosing, org.apache.hadoop.fs.RemoteIterator
				<org.apache.hadoop.fs.LocatedFileStatus> statusIter, org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult
				<org.apache.hadoop.fs.FileSystem> res, org.apache.hadoop.fs.Path f)
			{
				this._enclosing = _enclosing;
				this.statusIter = statusIter;
				this.res = res;
				this.f = f;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool hasNext()
			{
				return statusIter.hasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.LocatedFileStatus next()
			{
				org.apache.hadoop.fs.LocatedFileStatus status = statusIter.next();
				return (org.apache.hadoop.fs.LocatedFileStatus)org.apache.hadoop.fs.viewfs.ViewFileSystem
					.fixFileStatus(status, this._enclosing.getChrootedPath(res, status, f));
			}

			private readonly ViewFileSystem _enclosing;

			private readonly org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
				> statusIter;

			private readonly org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res;

			private readonly org.apache.hadoop.fs.Path f;
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.Path getChrootedPath(org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult
			<org.apache.hadoop.fs.FileSystem> res, org.apache.hadoop.fs.FileStatus status, org.apache.hadoop.fs.Path
			 f)
		{
			string suffix = ((org.apache.hadoop.fs.viewfs.ChRootedFileSystem)res.targetFileSystem
				).stripOutRoot(status.getPath());
			return this.makeQualified(suffix.Length == 0 ? f : new org.apache.hadoop.fs.Path(
				res.resolvedPath, suffix));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(dir), false);
			return res.targetFileSystem.mkdirs(res.remainingPath, permission);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.open(res.remainingPath, bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			// passing resolveLastComponet as false to catch renaming a mount point to 
			// itself. We need to catch this as an internal operation and fail.
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> resSrc = fsState.resolve(getUriPath(src), false);
			if (resSrc.isInternalDir())
			{
				throw readOnlyMountTable("rename", src);
			}
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> resDst = fsState.resolve(getUriPath(dst), false);
			if (resDst.isInternalDir())
			{
				throw readOnlyMountTable("rename", dst);
			}
			//
			// Alternate 3 : renames ONLY within the the same mount links.
			//
			if (resSrc.targetFileSystem != resDst.targetFileSystem)
			{
				throw new System.IO.IOException("Renames across Mount points not supported");
			}
			return resSrc.targetFileSystem.rename(resSrc.remainingPath, resDst.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.truncate(f, newLength);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			res.targetFileSystem.setOwner(res.remainingPath, username, groupname);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			res.targetFileSystem.setPermission(res.remainingPath, permission);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.setReplication(res.remainingPath, replication);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			res.targetFileSystem.setTimes(res.remainingPath, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.modifyAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeDefaultAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAcl(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.setAcl(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getAclStatus(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.setXAttr(res.remainingPath, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getXAttr(res.remainingPath, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getXAttrs(res.remainingPath, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.listXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeXAttr(res.remainingPath, name);
		}

		public override void setVerifyChecksum(bool verifyChecksum)
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
				<org.apache.hadoop.fs.FileSystem>> mountPoints = fsState.getMountPoints();
			foreach (org.apache.hadoop.fs.viewfs.InodeTree.MountPoint<org.apache.hadoop.fs.FileSystem
				> mount in mountPoints)
			{
				mount.target.targetFileSystem.setVerifyChecksum(verifyChecksum);
			}
		}

		public override long getDefaultBlockSize()
		{
			throw new org.apache.hadoop.fs.viewfs.NotInMountpointException("getDefaultBlockSize"
				);
		}

		public override short getDefaultReplication()
		{
			throw new org.apache.hadoop.fs.viewfs.NotInMountpointException("getDefaultReplication"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			throw new org.apache.hadoop.fs.viewfs.NotInMountpointException("getServerDefaults"
				);
		}

		public override long getDefaultBlockSize(org.apache.hadoop.fs.Path f)
		{
			try
			{
				org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
					> res = fsState.resolve(getUriPath(f), true);
				return res.targetFileSystem.getDefaultBlockSize(res.remainingPath);
			}
			catch (java.io.FileNotFoundException)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(f, "getDefaultBlockSize"
					);
			}
		}

		public override short getDefaultReplication(org.apache.hadoop.fs.Path f)
		{
			try
			{
				org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
					> res = fsState.resolve(getUriPath(f), true);
				return res.targetFileSystem.getDefaultReplication(res.remainingPath);
			}
			catch (java.io.FileNotFoundException)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(f, "getDefaultReplication"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.getServerDefaults(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.getContentSummary(res.remainingPath);
		}

		public override void setWriteChecksum(bool writeChecksum)
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
				<org.apache.hadoop.fs.FileSystem>> mountPoints = fsState.getMountPoints();
			foreach (org.apache.hadoop.fs.viewfs.InodeTree.MountPoint<org.apache.hadoop.fs.FileSystem
				> mount in mountPoints)
			{
				mount.target.targetFileSystem.setWriteChecksum(writeChecksum);
			}
		}

		public override org.apache.hadoop.fs.FileSystem[] getChildFileSystems()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
				<org.apache.hadoop.fs.FileSystem>> mountPoints = fsState.getMountPoints();
			System.Collections.Generic.ICollection<org.apache.hadoop.fs.FileSystem> children = 
				new java.util.HashSet<org.apache.hadoop.fs.FileSystem>();
			foreach (org.apache.hadoop.fs.viewfs.InodeTree.MountPoint<org.apache.hadoop.fs.FileSystem
				> mountPoint in mountPoints)
			{
				org.apache.hadoop.fs.FileSystem targetFs = mountPoint.target.targetFileSystem;
				Sharpen.Collections.AddAll(children, java.util.Arrays.asList(targetFs.getChildFileSystems
					()));
			}
			return Sharpen.Collections.ToArray(children, new org.apache.hadoop.fs.FileSystem[
				] {  });
		}

		public virtual org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint[] getMountPoints
			()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
				<org.apache.hadoop.fs.FileSystem>> mountPoints = fsState.getMountPoints();
			org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint[] result = new org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint
				[mountPoints.Count];
			for (int i = 0; i < mountPoints.Count; ++i)
			{
				result[i] = new org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint(new org.apache.hadoop.fs.Path
					(mountPoints[i].src), mountPoints[i].target.targetDirLinkList);
			}
			return result;
		}

		internal class InternalDirOfViewFs : org.apache.hadoop.fs.FileSystem
		{
			internal readonly org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<org.apache.hadoop.fs.FileSystem
				> theInternalDir;

			internal readonly long creationTime;

			internal readonly org.apache.hadoop.security.UserGroupInformation ugi;

			internal readonly java.net.URI myUri;

			/// <exception cref="java.net.URISyntaxException"/>
			public InternalDirOfViewFs(org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<org.apache.hadoop.fs.FileSystem
				> dir, long cTime, org.apache.hadoop.security.UserGroupInformation ugi, java.net.URI
				 uri)
			{
				/*
				* An instance of this class represents an internal dir of the viewFs
				* that is internal dir of the mount table.
				* It is a read only mount tables and create, mkdir or delete operations
				* are not allowed.
				* If called on create or mkdir then this target is the parent of the
				* directory in which one is trying to create or mkdir; hence
				* in this case the path name passed in is the last component.
				* Otherwise this target is the end point of the path and hence
				* the path name passed in is null.
				*/
				// of the the mount table
				// the user/group of user who created mtable
				myUri = uri;
				try
				{
					initialize(myUri, new org.apache.hadoop.conf.Configuration());
				}
				catch (System.IO.IOException)
				{
					throw new System.Exception("Cannot occur");
				}
				theInternalDir = dir;
				creationTime = cTime;
				this.ugi = ugi;
			}

			/// <exception cref="System.IO.IOException"/>
			private static void checkPathIsSlash(org.apache.hadoop.fs.Path f)
			{
				if (f != org.apache.hadoop.fs.viewfs.InodeTree.SlashPath)
				{
					throw new System.IO.IOException("Internal implementation error: expected file name to be /"
						);
				}
			}

			public override java.net.URI getUri()
			{
				return myUri;
			}

			public override org.apache.hadoop.fs.Path getWorkingDirectory()
			{
				throw new System.Exception("Internal impl error: getWorkingDir should not have been called"
					);
			}

			public override void setWorkingDirectory(org.apache.hadoop.fs.Path new_dir)
			{
				throw new System.Exception("Internal impl error: getWorkingDir should not have been called"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
				 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
			{
				throw readOnlyMountTable("append", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
				 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				throw readOnlyMountTable("create", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
			{
				checkPathIsSlash(f);
				throw readOnlyMountTable("delete", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool delete(org.apache.hadoop.fs.Path f)
			{
				return delete(f, true);
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus
				 fs, long start, long len)
			{
				checkPathIsSlash(fs.getPath());
				throw new java.io.FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
				 f)
			{
				checkPathIsSlash(f);
				throw new java.io.FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
				 f)
			{
				checkPathIsSlash(f);
				return new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, creationTime, creationTime
					, org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555, ugi.getUserName(), ugi.getGroupNames
					()[0], new org.apache.hadoop.fs.Path(theInternalDir.fullPath).makeQualified(myUri
					, ROOT_PATH));
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f)
			{
				checkPathIsSlash(f);
				org.apache.hadoop.fs.FileStatus[] result = new org.apache.hadoop.fs.FileStatus[theInternalDir
					.children.Count];
				int i = 0;
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.fs.viewfs.InodeTree.INode
					<org.apache.hadoop.fs.FileSystem>> iEntry in theInternalDir.children)
				{
					org.apache.hadoop.fs.viewfs.InodeTree.INode<org.apache.hadoop.fs.FileSystem> inode
						 = iEntry.Value;
					if (inode is org.apache.hadoop.fs.viewfs.InodeTree.INodeLink)
					{
						org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<org.apache.hadoop.fs.FileSystem> 
							link = (org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<org.apache.hadoop.fs.FileSystem
							>)inode;
						result[i++] = new org.apache.hadoop.fs.FileStatus(0, false, 0, 0, creationTime, creationTime
							, org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555, ugi.getUserName(), ugi.getGroupNames
							()[0], link.getTargetLink(), new org.apache.hadoop.fs.Path(inode.fullPath).makeQualified
							(myUri, null));
					}
					else
					{
						result[i++] = new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, creationTime, creationTime
							, org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555, ugi.getUserName(), ugi.getGroupNames
							()[0], new org.apache.hadoop.fs.Path(inode.fullPath).makeQualified(myUri, null));
					}
				}
				return result;
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
			public override bool mkdirs(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
				 permission)
			{
				if (theInternalDir.isRoot && dir == null)
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("/ already exits");
				}
				// Note dir starts with /
				if (theInternalDir.children.Contains(Sharpen.Runtime.substring(dir.ToString(), 1)
					))
				{
					return true;
				}
				// this is the stupid semantics of FileSystem
				throw readOnlyMountTable("mkdirs", dir);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
				 f, int bufferSize)
			{
				checkPathIsSlash(f);
				throw new java.io.FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
				checkPathIsSlash(src);
				checkPathIsSlash(dst);
				throw readOnlyMountTable("rename", src);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
			{
				throw readOnlyMountTable("truncate", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
				 groupname)
			{
				checkPathIsSlash(f);
				throw readOnlyMountTable("setOwner", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 permission)
			{
				checkPathIsSlash(f);
				throw readOnlyMountTable("setPermission", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
				)
			{
				checkPathIsSlash(f);
				throw readOnlyMountTable("setReplication", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
				)
			{
				checkPathIsSlash(f);
				throw readOnlyMountTable("setTimes", f);
			}

			public override void setVerifyChecksum(bool verifyChecksum)
			{
			}

			// Noop for viewfs
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path
				 f)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(f, "getServerDefaults"
					);
			}

			public override long getDefaultBlockSize(org.apache.hadoop.fs.Path f)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(f, "getDefaultBlockSize"
					);
			}

			public override short getDefaultReplication(org.apache.hadoop.fs.Path f)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(f, "getDefaultReplication"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
				<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("modifyAclEntries", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
				<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("removeAclEntries", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("removeDefaultAcl", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void removeAcl(org.apache.hadoop.fs.Path path)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("removeAcl", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
				<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("setAcl", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
				 path)
			{
				checkPathIsSlash(path);
				return new org.apache.hadoop.fs.permission.AclStatus.Builder().owner(ugi.getUserName
					()).group(ugi.getGroupNames()[0]).addEntries(org.apache.hadoop.fs.permission.AclUtil
					.getMinimalAcl(org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555)).stickyBit(
					false).build();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
				 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("setXAttr", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(path, "getXAttr");
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
				org.apache.hadoop.fs.Path path)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(path, "getXAttrs");
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
				org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(path, "getXAttrs");
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
				 path)
			{
				throw new org.apache.hadoop.fs.viewfs.NotInMountpointException(path, "listXAttrs"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
			{
				checkPathIsSlash(path);
				throw readOnlyMountTable("removeXAttr", path);
			}
		}
	}
}

using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// ViewFs (extends the AbstractFileSystem interface) implements a client-side
	/// mount table.
	/// </summary>
	/// <remarks>
	/// ViewFs (extends the AbstractFileSystem interface) implements a client-side
	/// mount table. The viewFs file system is implemented completely in memory on
	/// the client side. The client-side mount table allows a client to provide a
	/// customized view of a file system namespace that is composed from
	/// one or more individual file systems (a localFs or Hdfs, S3fs, etc).
	/// For example one could have a mount table that provides links such as
	/// <ul>
	/// <li>  /user          -&gt; hdfs://nnContainingUserDir/user
	/// <li>  /project/foo   -&gt; hdfs://nnProject1/projects/foo
	/// <li>  /project/bar   -&gt; hdfs://nnProject2/projects/bar
	/// <li>  /tmp           -&gt; hdfs://nnTmp/privateTmpForUserXXX
	/// </ul>
	/// ViewFs is specified with the following URI: <b>viewfs:///</b>
	/// <p>
	/// To use viewfs one would typically set the default file system in the
	/// config  (i.e. fs.default.name&lt; = viewfs:///) along with the
	/// mount table config variables as described below.
	/// <p>
	/// <b> ** Config variables to specify the mount table entries ** </b>
	/// <p>
	/// The file system is initialized from the standard Hadoop config through
	/// config variables.
	/// See
	/// <see cref="org.apache.hadoop.fs.FsConstants"/>
	/// for URI and Scheme constants;
	/// See
	/// <see cref="Constants"/>
	/// for config var constants;
	/// see
	/// <see cref="ConfigUtil"/>
	/// for convenient lib.
	/// <p>
	/// All the mount table config entries for view fs are prefixed by
	/// <b>fs.viewfs.mounttable.</b>
	/// For example the above example can be specified with the following
	/// config variables:
	/// <ul>
	/// <li> fs.viewfs.mounttable.default.link./user=
	/// hdfs://nnContainingUserDir/user
	/// <li> fs.viewfs.mounttable.default.link./project/foo=
	/// hdfs://nnProject1/projects/foo
	/// <li> fs.viewfs.mounttable.default.link./project/bar=
	/// hdfs://nnProject2/projects/bar
	/// <li> fs.viewfs.mounttable.default.link./tmp=
	/// hdfs://nnTmp/privateTmpForUserXXX
	/// </ul>
	/// The default mount table (when no authority is specified) is
	/// from config variables prefixed by <b>fs.viewFs.mounttable.default </b>
	/// The authority component of a URI can be used to specify a different mount
	/// table. For example,
	/// <ul>
	/// <li>  viewfs://sanjayMountable/
	/// </ul>
	/// is initialized from fs.viewFs.mounttable.sanjayMountable.* config variables.
	/// <p>
	/// <b> **** Merge Mounts **** </b>(NOTE: merge mounts are not implemented yet.)
	/// <p>
	/// One can also use "MergeMounts" to merge several directories (this is
	/// sometimes  called union-mounts or junction-mounts in the literature.
	/// For example of the home directories are stored on say two file systems
	/// (because they do not fit on one) then one could specify a mount
	/// entry such as following merges two dirs:
	/// <ul>
	/// <li> /user -&gt; hdfs://nnUser1/user,hdfs://nnUser2/user
	/// </ul>
	/// Such a mergeLink can be specified with the following config var where ","
	/// is used as the separator for each of links to be merged:
	/// <ul>
	/// <li> fs.viewfs.mounttable.default.linkMerge./user=
	/// hdfs://nnUser1/user,hdfs://nnUser1/user
	/// </ul>
	/// A special case of the merge mount is where mount table's root is merged
	/// with the root (slash) of another file system:
	/// <ul>
	/// <li>    fs.viewfs.mounttable.default.linkMergeSlash=hdfs://nn99/
	/// </ul>
	/// In this cases the root of the mount table is merged with the root of
	/// <b>hdfs://nn99/ </b>
	/// </remarks>
	public class ViewFs : org.apache.hadoop.fs.AbstractFileSystem
	{
		internal readonly long creationTime;

		internal readonly org.apache.hadoop.security.UserGroupInformation ugi;

		internal readonly org.apache.hadoop.conf.Configuration config;

		internal org.apache.hadoop.fs.viewfs.InodeTree<org.apache.hadoop.fs.AbstractFileSystem
			> fsState;

		internal org.apache.hadoop.fs.Path homeDir = null;

		/*Evolving for a release,to be changed to Stable */
		// of the the mount table
		// the user/group of user who created mtable
		// the fs state; ie the mount table
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		public ViewFs(org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, conf)
		{
		}

		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="org.apache.hadoop.fs.AbstractFileSystem.createFileSystem(java.net.URI, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="theUri">which must be that of ViewFs</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"></exception>
		internal ViewFs(java.net.URI theUri, org.apache.hadoop.conf.Configuration conf)
			: base(theUri, org.apache.hadoop.fs.FsConstants.VIEWFS_SCHEME, false, -1)
		{
			creationTime = org.apache.hadoop.util.Time.now();
			ugi = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
			config = conf;
			// Now build  client side view (i.e. client side mount table) from config.
			string authority = theUri.getAuthority();
			fsState = new _InodeTree_208(this, conf, authority);
		}

		private sealed class _InodeTree_208 : org.apache.hadoop.fs.viewfs.InodeTree<org.apache.hadoop.fs.AbstractFileSystem
			>
		{
			public _InodeTree_208(ViewFs _enclosing, org.apache.hadoop.conf.Configuration baseArg1
				, string baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="java.net.URISyntaxException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			protected internal override org.apache.hadoop.fs.AbstractFileSystem getTargetFileSystem
				(java.net.URI uri)
			{
				string pathString = uri.getPath();
				if (pathString.isEmpty())
				{
					pathString = "/";
				}
				return new org.apache.hadoop.fs.viewfs.ChRootedFs(org.apache.hadoop.fs.AbstractFileSystem
					.createFileSystem(uri, this._enclosing.config), new org.apache.hadoop.fs.Path(pathString
					));
			}

			/// <exception cref="java.net.URISyntaxException"/>
			protected internal override org.apache.hadoop.fs.AbstractFileSystem getTargetFileSystem
				(org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<org.apache.hadoop.fs.AbstractFileSystem
				> dir)
			{
				return new org.apache.hadoop.fs.viewfs.ViewFs.InternalDirOfViewFs(dir, this._enclosing
					.creationTime, this._enclosing.ugi, this._enclosing.getUri());
			}

			/// <exception cref="java.net.URISyntaxException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			protected internal override org.apache.hadoop.fs.AbstractFileSystem getTargetFileSystem
				(java.net.URI[] mergeFsURIList)
			{
				throw new org.apache.hadoop.fs.UnsupportedFileSystemException("mergefs not implemented yet"
					);
			}

			private readonly ViewFs _enclosing;
		}

		// return MergeFs.createMergeFs(mergeFsURIList, config);
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return org.apache.hadoop.fs.local.LocalConfigKeys.getServerDefaults();
		}

		public override int getUriDefaultPort()
		{
			return -1;
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

		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path f
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res;
			res = fsState.resolve(getUriPath(f), true);
			if (res.isInternalDir())
			{
				return f;
			}
			return res.targetFileSystem.resolvePath(res.remainingPath);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res;
			try
			{
				res = fsState.resolve(getUriPath(f), false);
			}
			catch (java.io.FileNotFoundException e)
			{
				if (createParent)
				{
					throw readOnlyMountTable("create", f);
				}
				else
				{
					throw;
				}
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			return res.targetFileSystem.createInternal(res.remainingPath, flag, absolutePermission
				, bufferSize, replication, blockSize, progress, checksumOpt, createParent);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			// If internal dir or target is a mount link (ie remainingPath is Slash)
			if (res.isInternalDir() || res.remainingPath == org.apache.hadoop.fs.viewfs.InodeTree
				.SlashPath)
			{
				throw new org.apache.hadoop.security.AccessControlException("Cannot delete internal mount table directory: "
					 + f);
			}
			return res.targetFileSystem.delete(res.remainingPath, recursive);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.getFileBlockLocations(res.remainingPath, start, len);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.getFileChecksum(res.remainingPath);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			//  FileStatus#getPath is a fully qualified path relative to the root of 
			// target file system.
			// We need to change it to viewfs URI - relative to root of mount table.
			// The implementors of RawLocalFileSystem were trying to be very smart.
			// They implement FileStatus#getOwener lazily -- the object
			// returned is really a RawLocalFileSystem that expect the
			// FileStatus#getPath to be unchanged so that it can get owner when needed.
			// Hence we need to interpose a new ViewFsFileStatus that works around.
			org.apache.hadoop.fs.FileStatus status = res.targetFileSystem.getFileStatus(res.remainingPath
				);
			return new org.apache.hadoop.fs.viewfs.ViewFsFileStatus(status, this.makeQualified
				(f));
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.access(res.remainingPath, mode);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), false);
			// do not follow mount link
			return res.targetFileSystem.getFileLinkStatus(res.remainingPath);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getFsStatus()
		{
			return new org.apache.hadoop.fs.FsStatus(0, 0, 0);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> fsIter = res
				.targetFileSystem.listStatusIterator(res.remainingPath);
			if (res.isInternalDir())
			{
				return fsIter;
			}
			return new _RemoteIterator_391(this, fsIter, res, f);
		}

		private sealed class _RemoteIterator_391 : org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			>
		{
			public _RemoteIterator_391(ViewFs _enclosing, org.apache.hadoop.fs.RemoteIterator
				<org.apache.hadoop.fs.FileStatus> fsIter, org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult
				<org.apache.hadoop.fs.AbstractFileSystem> res, org.apache.hadoop.fs.Path f)
			{
				this._enclosing = _enclosing;
				this.fsIter = fsIter;
				this.res = res;
				this.f = f;
				{
					// Init
					this.myIter = fsIter;
					this.targetFs = (org.apache.hadoop.fs.viewfs.ChRootedFs)res.targetFileSystem;
				}
			}

			internal readonly org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
				> myIter;

			internal readonly org.apache.hadoop.fs.viewfs.ChRootedFs targetFs;

			/// <exception cref="System.IO.IOException"/>
			public bool hasNext()
			{
				return this.myIter.hasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.fs.FileStatus next()
			{
				org.apache.hadoop.fs.FileStatus status = this.myIter.next();
				string suffix = this.targetFs.stripOutRoot(status.getPath());
				return new org.apache.hadoop.fs.viewfs.ViewFsFileStatus(status, this._enclosing.makeQualified
					(suffix.Length == 0 ? f : new org.apache.hadoop.fs.Path(res.resolvedPath, suffix
					)));
			}

			private readonly ViewFs _enclosing;

			private readonly org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
				> fsIter;

			private readonly org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res;

			private readonly org.apache.hadoop.fs.Path f;
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			org.apache.hadoop.fs.FileStatus[] statusLst = res.targetFileSystem.listStatus(res
				.remainingPath);
			if (!res.isInternalDir())
			{
				// We need to change the name in the FileStatus as described in
				// {@link #getFileStatus }
				org.apache.hadoop.fs.viewfs.ChRootedFs targetFs;
				targetFs = (org.apache.hadoop.fs.viewfs.ChRootedFs)res.targetFileSystem;
				int i = 0;
				foreach (org.apache.hadoop.fs.FileStatus status in statusLst)
				{
					string suffix = targetFs.stripOutRoot(status.getPath());
					statusLst[i++] = new org.apache.hadoop.fs.viewfs.ViewFsFileStatus(status, this.makeQualified
						(suffix.Length == 0 ? f : new org.apache.hadoop.fs.Path(res.resolvedPath, suffix
						)));
				}
			}
			return statusLst;
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(dir), false);
			res.targetFileSystem.mkdir(res.remainingPath, permission, createParent);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.open(res.remainingPath, bufferSize);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.truncate(res.remainingPath, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool overwrite)
		{
			// passing resolveLastComponet as false to catch renaming a mount point 
			// itself we need to catch this as an internal operation and fail.
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> resSrc = fsState.resolve(getUriPath(src), false);
			if (resSrc.isInternalDir())
			{
				throw new org.apache.hadoop.security.AccessControlException("Cannot Rename within internal dirs of mount table: it is readOnly"
					);
			}
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> resDst = fsState.resolve(getUriPath(dst), false);
			if (resDst.isInternalDir())
			{
				throw new org.apache.hadoop.security.AccessControlException("Cannot Rename within internal dirs of mount table: it is readOnly"
					);
			}
			//
			// Alternate 3 : renames ONLY within the the same mount links.
			//
			if (resSrc.targetFileSystem != resDst.targetFileSystem)
			{
				throw new System.IO.IOException("Renames across Mount points not supported");
			}
			resSrc.targetFileSystem.renameInternal(resSrc.remainingPath, resDst.remainingPath
				, overwrite);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			renameInternal(src, dst, false);
		}

		public override bool supportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res;
			try
			{
				res = fsState.resolve(getUriPath(link), false);
			}
			catch (java.io.FileNotFoundException e)
			{
				if (createParent)
				{
					throw readOnlyMountTable("createSymlink", link);
				}
				else
				{
					throw;
				}
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			res.targetFileSystem.createSymlink(target, res.remainingPath, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), false);
			// do not follow mount link
			return res.targetFileSystem.getLinkTarget(res.remainingPath);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			res.targetFileSystem.setOwner(res.remainingPath, username, groupname);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			res.targetFileSystem.setPermission(res.remainingPath, permission);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			return res.targetFileSystem.setReplication(res.remainingPath, replication);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(f), true);
			res.targetFileSystem.setTimes(res.remainingPath, mtime, atime);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setVerifyChecksum(bool verifyChecksum)
		{
		}

		// This is a file system level operations, however ViewFs 
		// points to many file systems. Noop for ViewFs. 
		public virtual org.apache.hadoop.fs.viewfs.ViewFs.MountPoint[] getMountPoints()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
				<org.apache.hadoop.fs.AbstractFileSystem>> mountPoints = fsState.getMountPoints(
				);
			org.apache.hadoop.fs.viewfs.ViewFs.MountPoint[] result = new org.apache.hadoop.fs.viewfs.ViewFs.MountPoint
				[mountPoints.Count];
			for (int i = 0; i < mountPoints.Count; ++i)
			{
				result[i] = new org.apache.hadoop.fs.viewfs.ViewFs.MountPoint(new org.apache.hadoop.fs.Path
					(mountPoints[i].src), mountPoints[i].target.targetDirLinkList);
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> getDelegationTokens(string renewer)
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
				<org.apache.hadoop.fs.AbstractFileSystem>> mountPoints = fsState.getMountPoints(
				);
			int initialListSize = 0;
			foreach (org.apache.hadoop.fs.viewfs.InodeTree.MountPoint<org.apache.hadoop.fs.AbstractFileSystem
				> im in mountPoints)
			{
				initialListSize += im.target.targetDirLinkList.Length;
			}
			System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
				result = new System.Collections.Generic.List<org.apache.hadoop.security.token.Token
				<object>>(initialListSize);
			for (int i = 0; i < mountPoints.Count; ++i)
			{
				System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
					tokens = mountPoints[i].target.targetFileSystem.getDelegationTokens(renewer);
				if (tokens != null)
				{
					Sharpen.Collections.AddAll(result, tokens);
				}
			}
			return result;
		}

		public override bool isValidName(string src)
		{
			// Prefix validated at mount time and rest of path validated by mount target.
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.modifyAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeDefaultAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAcl(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.setAcl(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getAclStatus(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.setXAttr(res.remainingPath, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getXAttr(res.remainingPath, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.getXAttrs(res.remainingPath, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			return res.targetFileSystem.listXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = fsState.resolve(getUriPath(path), true);
			res.targetFileSystem.removeXAttr(res.remainingPath, name);
		}

		internal class InternalDirOfViewFs : org.apache.hadoop.fs.AbstractFileSystem
		{
			internal readonly org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<org.apache.hadoop.fs.AbstractFileSystem
				> theInternalDir;

			internal readonly long creationTime;

			internal readonly org.apache.hadoop.security.UserGroupInformation ugi;

			internal readonly java.net.URI myUri;

			/// <exception cref="java.net.URISyntaxException"/>
			public InternalDirOfViewFs(org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<org.apache.hadoop.fs.AbstractFileSystem
				> dir, long cTime, org.apache.hadoop.security.UserGroupInformation ugi, java.net.URI
				 uri)
				: base(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, org.apache.hadoop.fs.FsConstants
					.VIEWFS_SCHEME, false, -1)
			{
				/*
				* An instance of this class represents an internal dir of the viewFs
				* ie internal dir of the mount table.
				* It is a ready only mount tbale and create, mkdir or delete operations
				* are not allowed.
				* If called on create or mkdir then this target is the parent of the
				* directory in which one is trying to create or mkdir; hence
				* in this case the path name passed in is the last component.
				* Otherwise this target is the end point of the path and hence
				* the path name passed in is null.
				*/
				// of the the mount table
				// the user/group of user who created mtable
				// the URI of the outer ViewFs
				theInternalDir = dir;
				creationTime = cTime;
				this.ugi = ugi;
				myUri = uri;
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

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
				 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
				 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
				)
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

			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
				 f, long start, long len)
			{
				checkPathIsSlash(f);
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
					, null));
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
				 f)
			{
				// look up i internalDirs children - ignore first Slash
				org.apache.hadoop.fs.viewfs.InodeTree.INode<org.apache.hadoop.fs.AbstractFileSystem
					> inode = theInternalDir.children[Sharpen.Runtime.substring(f.toUri().ToString()
					, 1)];
				if (inode == null)
				{
					throw new java.io.FileNotFoundException("viewFs internal mount table - missing entry:"
						 + f);
				}
				org.apache.hadoop.fs.FileStatus result;
				if (inode is org.apache.hadoop.fs.viewfs.InodeTree.INodeLink)
				{
					org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<org.apache.hadoop.fs.AbstractFileSystem
						> inodelink = (org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<org.apache.hadoop.fs.AbstractFileSystem
						>)inode;
					result = new org.apache.hadoop.fs.FileStatus(0, false, 0, 0, creationTime, creationTime
						, org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555, ugi.getUserName(), ugi.getGroupNames
						()[0], inodelink.getTargetLink(), new org.apache.hadoop.fs.Path(inode.fullPath).
						makeQualified(myUri, null));
				}
				else
				{
					result = new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, creationTime, creationTime
						, org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555, ugi.getUserName(), ugi.getGroupNames
						()[0], new org.apache.hadoop.fs.Path(inode.fullPath).makeQualified(myUri, null));
				}
				return result;
			}

			public override org.apache.hadoop.fs.FsStatus getFsStatus()
			{
				return new org.apache.hadoop.fs.FsStatus(0, 0, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
			{
				throw new System.IO.IOException("FsServerDefaults not implemented yet");
			}

			public override int getUriDefaultPort()
			{
				return -1;
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f)
			{
				checkPathIsSlash(f);
				org.apache.hadoop.fs.FileStatus[] result = new org.apache.hadoop.fs.FileStatus[theInternalDir
					.children.Count];
				int i = 0;
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.fs.viewfs.InodeTree.INode
					<org.apache.hadoop.fs.AbstractFileSystem>> iEntry in theInternalDir.children)
				{
					org.apache.hadoop.fs.viewfs.InodeTree.INode<org.apache.hadoop.fs.AbstractFileSystem
						> inode = iEntry.Value;
					if (inode is org.apache.hadoop.fs.viewfs.InodeTree.INodeLink)
					{
						org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<org.apache.hadoop.fs.AbstractFileSystem
							> link = (org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<org.apache.hadoop.fs.AbstractFileSystem
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
			public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
				 permission, bool createParent)
			{
				if (theInternalDir.isRoot && dir == null)
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("/ already exits");
				}
				throw readOnlyMountTable("mkdir", dir);
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
				 f, int bufferSize)
			{
				checkPathIsSlash(f);
				throw new java.io.FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
			{
				checkPathIsSlash(f);
				throw readOnlyMountTable("truncate", f);
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
				checkPathIsSlash(src);
				checkPathIsSlash(dst);
				throw readOnlyMountTable("rename", src);
			}

			public override bool supportsSymlinks()
			{
				return true;
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
				 link, bool createParent)
			{
				throw readOnlyMountTable("createSymlink", link);
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
				 f)
			{
				return getFileLinkStatus(f).getSymlink();
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

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			public override void setVerifyChecksum(bool verifyChecksum)
			{
				throw readOnlyMountTable("setVerifyChecksum", string.Empty);
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

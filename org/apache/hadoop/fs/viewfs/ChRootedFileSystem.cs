using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// <code>ChRootedFileSystem</code> is a file system with its root some path
	/// below the root of its base file system.
	/// </summary>
	/// <remarks>
	/// <code>ChRootedFileSystem</code> is a file system with its root some path
	/// below the root of its base file system.
	/// Example: For a base file system hdfs://nn1/ with chRoot at /usr/foo, the
	/// members will be setup as shown below.
	/// <ul>
	/// <li>myFs is the base file system and points to hdfs at nn1</li>
	/// <li>myURI is hdfs://nn1/user/foo</li>
	/// <li>chRootPathPart is /user/foo</li>
	/// <li>workingDir is a directory related to chRoot</li>
	/// </ul>
	/// The paths are resolved as follows by ChRootedFileSystem:
	/// <ul>
	/// <li> Absolute path /a/b/c is resolved to /user/foo/a/b/c at myFs</li>
	/// <li> Relative path x/y is resolved to /user/foo/<workingDir>/x/y</li>
	/// </ul>
	/// </remarks>
	internal class ChRootedFileSystem : org.apache.hadoop.fs.FilterFileSystem
	{
		private readonly java.net.URI myUri;

		private readonly org.apache.hadoop.fs.Path chRootPathPart;

		private readonly string chRootPathPartString;

		private org.apache.hadoop.fs.Path workingDir;

		/*Evolving for a release,to be changed to Stable */
		// the base URI + the chRoot
		// the root below the root of the base
		protected internal virtual org.apache.hadoop.fs.FileSystem getMyFs()
		{
			return getRawFileSystem();
		}

		/// <param name="path"/>
		/// <returns>full path including the chroot</returns>
		protected internal virtual org.apache.hadoop.fs.Path fullPath(org.apache.hadoop.fs.Path
			 path)
		{
			base.checkPath(path);
			return path.isAbsolute() ? new org.apache.hadoop.fs.Path((chRootPathPart.isRoot()
				 ? string.Empty : chRootPathPartString) + path.toUri().getPath()) : new org.apache.hadoop.fs.Path
				(chRootPathPartString + workingDir.toUri().getPath(), path);
		}

		/// <summary>Constructor</summary>
		/// <param name="uri">base file system</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"></exception>
		public ChRootedFileSystem(java.net.URI uri, org.apache.hadoop.conf.Configuration 
			conf)
			: base(org.apache.hadoop.fs.FileSystem.get(uri, conf))
		{
			string pathString = uri.getPath();
			if (pathString.isEmpty())
			{
				pathString = "/";
			}
			chRootPathPart = new org.apache.hadoop.fs.Path(pathString);
			chRootPathPartString = chRootPathPart.toUri().getPath();
			myUri = uri;
			workingDir = getHomeDirectory();
		}

		// We don't use the wd of the myFs
		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="name">
		/// a uri whose authority section names the host, port, etc.
		/// for this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public override void initialize(java.net.URI name, org.apache.hadoop.conf.Configuration
			 conf)
		{
			base.initialize(name, conf);
			setConf(conf);
		}

		public override java.net.URI getUri()
		{
			return myUri;
		}

		/// <summary>Strip out the root from the path.</summary>
		/// <param name="p">- fully qualified path p</param>
		/// <returns>-  the remaining path  without the begining /</returns>
		/// <exception cref="System.IO.IOException">if the p is not prefixed with root</exception>
		internal virtual string stripOutRoot(org.apache.hadoop.fs.Path p)
		{
			try
			{
				checkPath(p);
			}
			catch (System.ArgumentException)
			{
				throw new System.IO.IOException("Internal Error - path " + p + " should have been with URI: "
					 + myUri);
			}
			string pathPart = p.toUri().getPath();
			return (pathPart.Length == chRootPathPartString.Length) ? string.Empty : Sharpen.Runtime.substring
				(pathPart, chRootPathPartString.Length + (chRootPathPart.isRoot() ? 0 : 1));
		}

		protected internal override org.apache.hadoop.fs.Path getInitialWorkingDirectory(
			)
		{
			/*
			* 3 choices here:
			*     null or / or /user/<uname> or strip out the root out of myFs's
			*  inital wd.
			* Only reasonable choice for initialWd for chrooted fds is null
			* so that the default rule for wd is applied
			*/
			return null;
		}

		/// <exception cref="java.io.FileNotFoundException"/>
		public virtual org.apache.hadoop.fs.Path getResolvedQualifiedPath(org.apache.hadoop.fs.Path
			 f)
		{
			return makeQualified(new org.apache.hadoop.fs.Path(chRootPathPartString + f.toUri
				().ToString()));
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return workingDir;
		}

		public override void setWorkingDirectory(org.apache.hadoop.fs.Path new_dir)
		{
			workingDir = new_dir.isAbsolute() ? new_dir : new org.apache.hadoop.fs.Path(workingDir
				, new_dir);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return base.create(fullPath(f), permission, overwrite, bufferSize, replication, blockSize
				, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return base.createNonRecursive(fullPath(f), permission, flags, bufferSize, replication
				, blockSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			return base.delete(fullPath(f), recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f)
		{
			return delete(f, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus
			 fs, long start, long len)
		{
			return base.getFileBlockLocations(new org.apache.hadoop.fs.viewfs.ViewFsFileStatus
				(fs, fullPath(fs.getPath())), start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			return base.getFileChecksum(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return base.getFileStatus(fullPath(f));
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			base.access(fullPath(path), mode);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getStatus(org.apache.hadoop.fs.Path
			 p)
		{
			return base.getStatus(fullPath(p));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return base.listStatus(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f)
		{
			return base.listLocatedStatus(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			return base.mkdirs(fullPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			return base.open(fullPath(f), bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			return base.append(fullPath(f), bufferSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			// note fullPath will check that paths are relative to this FileSystem.
			// Hence both are in same file system and a rename is valid
			return base.rename(fullPath(src), fullPath(dst));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			base.setOwner(fullPath(f), username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			base.setPermission(fullPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			return base.setReplication(fullPath(f), replication);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			base.setTimes(fullPath(f), mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			base.modifyAclEntries(fullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			base.removeAclEntries(fullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			base.removeDefaultAcl(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAcl(org.apache.hadoop.fs.Path path)
		{
			base.removeAcl(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			base.setAcl(fullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			return base.getAclStatus(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			base.setXAttr(fullPath(path), name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			return base.getXAttr(fullPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path)
		{
			return base.getXAttrs(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
		{
			return base.getXAttrs(fullPath(path), names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			return base.listXAttrs(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			base.removeXAttr(fullPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p
			)
		{
			return base.resolvePath(fullPath(p));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getContentSummary(fullPath(f));
		}

		private static org.apache.hadoop.fs.Path rootPath = new org.apache.hadoop.fs.Path
			(org.apache.hadoop.fs.Path.SEPARATOR);

		public override long getDefaultBlockSize()
		{
			return getDefaultBlockSize(fullPath(rootPath));
		}

		public override long getDefaultBlockSize(org.apache.hadoop.fs.Path f)
		{
			return base.getDefaultBlockSize(fullPath(f));
		}

		public override short getDefaultReplication()
		{
			return getDefaultReplication(fullPath(rootPath));
		}

		public override short getDefaultReplication(org.apache.hadoop.fs.Path f)
		{
			return base.getDefaultReplication(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return getServerDefaults(fullPath(rootPath));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path
			 f)
		{
			return base.getServerDefaults(fullPath(f));
		}
	}
}

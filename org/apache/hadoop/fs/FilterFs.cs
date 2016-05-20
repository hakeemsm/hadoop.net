using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// A <code>FilterFs</code> contains some other file system, which it uses as its
	/// basic file system, possibly transforming the data along the way or providing
	/// additional functionality.
	/// </summary>
	/// <remarks>
	/// A <code>FilterFs</code> contains some other file system, which it uses as its
	/// basic file system, possibly transforming the data along the way or providing
	/// additional functionality. The class <code>FilterFs</code> itself simply
	/// overrides all methods of <code>AbstractFileSystem</code> with versions that
	/// pass all requests to the contained file system. Subclasses of
	/// <code>FilterFs</code> may further override some of these methods and may also
	/// provide additional methods and fields.
	/// </remarks>
	public abstract class FilterFs : org.apache.hadoop.fs.AbstractFileSystem
	{
		private readonly org.apache.hadoop.fs.AbstractFileSystem myFs;

		/*Evolving for a release,to be changed to Stable */
		protected internal virtual org.apache.hadoop.fs.AbstractFileSystem getMyFs()
		{
			return myFs;
		}

		/// <exception cref="java.net.URISyntaxException"/>
		protected internal FilterFs(org.apache.hadoop.fs.AbstractFileSystem fs)
			: base(fs.getUri(), fs.getUri().getScheme(), fs.getUri().getAuthority() != null, 
				fs.getUriDefaultPort())
		{
			myFs = fs;
		}

		public override org.apache.hadoop.fs.FileSystem.Statistics getStatistics()
		{
			return myFs.getStatistics();
		}

		public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 path)
		{
			return myFs.makeQualified(path);
		}

		public override org.apache.hadoop.fs.Path getInitialWorkingDirectory()
		{
			return myFs.getInitialWorkingDirectory();
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return myFs.getHomeDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
			)
		{
			checkPath(f);
			return myFs.createInternal(f, flag, absolutePermission, bufferSize, replication, 
				blockSize, progress, checksumOpt, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			checkPath(f);
			return myFs.delete(f, recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			checkPath(f);
			return myFs.getFileBlockLocations(f, start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return myFs.getFileChecksum(f);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return myFs.getFileStatus(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			checkPath(path);
			myFs.access(path, mode);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return myFs.getFileLinkStatus(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getFsStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.getFsStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getFsStatus()
		{
			return myFs.getFsStatus();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return myFs.getServerDefaults();
		}

		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p
			)
		{
			return myFs.resolvePath(p);
		}

		public override int getUriDefaultPort()
		{
			return myFs.getUriDefaultPort();
		}

		public override java.net.URI getUri()
		{
			return myFs.getUri();
		}

		public override void checkPath(org.apache.hadoop.fs.Path path)
		{
			myFs.checkPath(path);
		}

		public override string getUriPath(org.apache.hadoop.fs.Path p)
		{
			return myFs.getUriPath(p);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return myFs.listStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.Path> listCorruptFileBlocks
			(org.apache.hadoop.fs.Path path)
		{
			return myFs.listCorruptFileBlocks(path);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent)
		{
			checkPath(dir);
			myFs.mkdir(dir, permission, createParent);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return myFs.open(f);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			checkPath(f);
			return myFs.open(f, bufferSize);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			checkPath(f);
			return myFs.truncate(f, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			checkPath(src);
			checkPath(dst);
			myFs.rename(src, dst, org.apache.hadoop.fs.Options.Rename.NONE);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool overwrite)
		{
			myFs.renameInternal(src, dst, overwrite);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			checkPath(f);
			myFs.setOwner(f, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			checkPath(f);
			myFs.setPermission(f, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			checkPath(f);
			return myFs.setReplication(f, replication);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			checkPath(f);
			myFs.setTimes(f, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setVerifyChecksum(bool verifyChecksum)
		{
			myFs.setVerifyChecksum(verifyChecksum);
		}

		public override bool supportsSymlinks()
		{
			return myFs.supportsSymlinks();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			myFs.createSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.getLinkTarget(f);
		}

		public override string getCanonicalServiceName()
		{
			// AbstractFileSystem
			return myFs.getCanonicalServiceName();
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> getDelegationTokens(string renewer)
		{
			// AbstractFileSystem
			return myFs.getDelegationTokens(renewer);
		}

		public override bool isValidName(string src)
		{
			return myFs.isValidName(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			myFs.modifyAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			myFs.removeAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			myFs.removeDefaultAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAcl(org.apache.hadoop.fs.Path path)
		{
			myFs.removeAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			myFs.setAcl(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			return myFs.getAclStatus(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value)
		{
			myFs.setXAttr(path, name, value);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			myFs.setXAttr(path, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			return myFs.getXAttr(path, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path)
		{
			return myFs.getXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
		{
			return myFs.getXAttrs(path, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			return myFs.listXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			myFs.removeXAttr(path, name);
		}
	}
}

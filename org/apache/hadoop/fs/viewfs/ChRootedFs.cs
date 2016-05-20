using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// <code>ChrootedFs</code> is a file system with its root some path
	/// below the root of its base file system.
	/// </summary>
	/// <remarks>
	/// <code>ChrootedFs</code> is a file system with its root some path
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
	internal class ChRootedFs : org.apache.hadoop.fs.AbstractFileSystem
	{
		private readonly org.apache.hadoop.fs.AbstractFileSystem myFs;

		private readonly java.net.URI myUri;

		private readonly org.apache.hadoop.fs.Path chRootPathPart;

		private readonly string chRootPathPartString;

		/*Evolving for a release,to be changed to Stable */
		// the base file system whose root is changed
		// the base URI + the chroot
		// the root below the root of the base
		protected internal virtual org.apache.hadoop.fs.AbstractFileSystem getMyFs()
		{
			return myFs;
		}

		/// <param name="path"/>
		/// <returns>return full path including the chroot</returns>
		protected internal virtual org.apache.hadoop.fs.Path fullPath(org.apache.hadoop.fs.Path
			 path)
		{
			base.checkPath(path);
			return new org.apache.hadoop.fs.Path((chRootPathPart.isRoot() ? string.Empty : chRootPathPartString
				) + path.toUri().getPath());
		}

		public override bool isValidName(string src)
		{
			return myFs.isValidName(fullPath(new org.apache.hadoop.fs.Path(src)).toUri().ToString
				());
		}

		/// <exception cref="java.net.URISyntaxException"/>
		public ChRootedFs(org.apache.hadoop.fs.AbstractFileSystem fs, org.apache.hadoop.fs.Path
			 theRoot)
			: base(fs.getUri(), fs.getUri().getScheme(), fs.getUri().getAuthority() != null, 
				fs.getUriDefaultPort())
		{
			myFs = fs;
			myFs.checkPath(theRoot);
			chRootPathPart = new org.apache.hadoop.fs.Path(myFs.getUriPath(theRoot));
			chRootPathPartString = chRootPathPart.toUri().getPath();
			/*
			* We are making URI include the chrootedPath: e.g. file:///chrootedPath.
			* This is questionable since Path#makeQualified(uri, path) ignores
			* the pathPart of a uri. Since this class is internal we can ignore
			* this issue but if we were to make it external then this needs
			* to be resolved.
			*/
			// Handle the two cases:
			//              scheme:/// and scheme://authority/
			myUri = new java.net.URI(myFs.getUri().ToString() + (myFs.getUri().getAuthority()
				 == null ? string.Empty : org.apache.hadoop.fs.Path.SEPARATOR) + Sharpen.Runtime.substring
				(chRootPathPart.toUri().getPath(), 1));
			base.checkPath(theRoot);
		}

		public override java.net.URI getUri()
		{
			return myUri;
		}

		/// <summary>Strip out the root from the path.</summary>
		/// <param name="p">- fully qualified path p</param>
		/// <returns>-  the remaining path  without the begining /</returns>
		public virtual string stripOutRoot(org.apache.hadoop.fs.Path p)
		{
			try
			{
				checkPath(p);
			}
			catch (System.ArgumentException)
			{
				throw new System.Exception("Internal Error - path " + p + " should have been with URI"
					 + myUri);
			}
			string pathPart = p.toUri().getPath();
			return (pathPart.Length == chRootPathPartString.Length) ? string.Empty : Sharpen.Runtime.substring
				(pathPart, chRootPathPartString.Length + (chRootPathPart.isRoot() ? 0 : 1));
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return myFs.getHomeDirectory();
		}

		public override org.apache.hadoop.fs.Path getInitialWorkingDirectory()
		{
			/*
			* 3 choices here: return null or / or strip out the root out of myFs's
			*  inital wd.
			* Only reasonable choice for initialWd for chrooted fds is null
			*/
			return null;
		}

		/// <exception cref="java.io.FileNotFoundException"/>
		public virtual org.apache.hadoop.fs.Path getResolvedQualifiedPath(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.makeQualified(new org.apache.hadoop.fs.Path(chRootPathPartString + f.
				toUri().ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
			)
		{
			return myFs.createInternal(fullPath(f), flag, absolutePermission, bufferSize, replication
				, blockSize, progress, checksumOpt, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			return myFs.delete(fullPath(f), recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			return myFs.getFileBlockLocations(fullPath(f), start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.getFileChecksum(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.getFileStatus(fullPath(f));
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			myFs.access(fullPath(path), mode);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.getFileLinkStatus(fullPath(f));
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

		public override int getUriDefaultPort()
		{
			return myFs.getUriDefaultPort();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.listStatus(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent)
		{
			myFs.mkdir(fullPath(dir), permission, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			return myFs.open(fullPath(f), bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			return myFs.truncate(fullPath(f), newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			// note fullPath will check that paths are relative to this FileSystem.
			// Hence both are in same file system and a rename is valid
			myFs.renameInternal(fullPath(src), fullPath(dst));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool overwrite)
		{
			// note fullPath will check that paths are relative to this FileSystem.
			// Hence both are in same file system and a rename is valid
			myFs.renameInternal(fullPath(src), fullPath(dst), overwrite);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			myFs.setOwner(fullPath(f), username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			myFs.setPermission(fullPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			return myFs.setReplication(fullPath(f), replication);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			myFs.setTimes(fullPath(f), mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			myFs.modifyAclEntries(fullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			myFs.removeAclEntries(fullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			myFs.removeDefaultAcl(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAcl(org.apache.hadoop.fs.Path path)
		{
			myFs.removeAcl(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			myFs.setAcl(fullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			return myFs.getAclStatus(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			myFs.setXAttr(fullPath(path), name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			return myFs.getXAttr(fullPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path)
		{
			return myFs.getXAttrs(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
		{
			return myFs.getXAttrs(fullPath(path), names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			return myFs.listXAttrs(fullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			myFs.removeXAttr(fullPath(path), name);
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
			/*
			* We leave the link alone:
			* If qualified or link relative then of course it is okay.
			* If absolute (ie / relative) then the link has to be resolved
			* relative to the changed root.
			*/
			myFs.createSymlink(fullPath(target), link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return myFs.getLinkTarget(fullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> getDelegationTokens(string renewer)
		{
			return myFs.getDelegationTokens(renewer);
		}
	}
}

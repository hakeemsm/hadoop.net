using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// A <code>FilterFileSystem</code> contains
	/// some other file system, which it uses as
	/// its  basic file system, possibly transforming
	/// the data along the way or providing  additional
	/// functionality.
	/// </summary>
	/// <remarks>
	/// A <code>FilterFileSystem</code> contains
	/// some other file system, which it uses as
	/// its  basic file system, possibly transforming
	/// the data along the way or providing  additional
	/// functionality. The class <code>FilterFileSystem</code>
	/// itself simply overrides all  methods of
	/// <code>FileSystem</code> with versions that
	/// pass all requests to the contained  file
	/// system. Subclasses of <code>FilterFileSystem</code>
	/// may further override some of  these methods
	/// and may also provide additional methods
	/// and fields.
	/// </remarks>
	public class FilterFileSystem : org.apache.hadoop.fs.FileSystem
	{
		protected internal org.apache.hadoop.fs.FileSystem fs;

		protected internal string swapScheme;

		public FilterFileSystem()
		{
		}

		public FilterFileSystem(org.apache.hadoop.fs.FileSystem fs)
		{
			/*
			* so that extending classes can define it
			*/
			this.fs = fs;
			this.statistics = fs.statistics;
		}

		/// <summary>Get the raw file system</summary>
		/// <returns>FileSystem being filtered</returns>
		public virtual org.apache.hadoop.fs.FileSystem getRawFileSystem()
		{
			return fs;
		}

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
			// this is less than ideal, but existing filesystems sometimes neglect
			// to initialize the embedded filesystem
			if (fs.getConf() == null)
			{
				fs.initialize(name, conf);
			}
			string scheme = name.getScheme();
			if (!scheme.Equals(fs.getUri().getScheme()))
			{
				swapScheme = scheme;
			}
		}

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		public override java.net.URI getUri()
		{
			return fs.getUri();
		}

		protected internal override java.net.URI getCanonicalUri()
		{
			return fs.getCanonicalUri();
		}

		protected internal override java.net.URI canonicalizeUri(java.net.URI uri)
		{
			return fs.canonicalizeUri(uri);
		}

		/// <summary>Make sure that a path specifies a FileSystem.</summary>
		public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.Path fqPath = fs.makeQualified(path);
			// swap in our scheme if the filtered fs is using a different scheme
			if (swapScheme != null)
			{
				try
				{
					// NOTE: should deal with authority, but too much other stuff is broken 
					fqPath = new org.apache.hadoop.fs.Path(new java.net.URI(swapScheme, fqPath.toUri(
						).getSchemeSpecificPart(), null));
				}
				catch (java.net.URISyntaxException e)
				{
					throw new System.ArgumentException(e);
				}
			}
			return fqPath;
		}

		///////////////////////////////////////////////////////////////
		// FileSystem
		///////////////////////////////////////////////////////////////
		/// <summary>Check that a Path belongs to this FileSystem.</summary>
		protected internal override void checkPath(org.apache.hadoop.fs.Path path)
		{
			fs.checkPath(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus
			 file, long start, long len)
		{
			return fs.getFileBlockLocations(file, start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p
			)
		{
			return fs.resolvePath(p);
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			return fs.open(f, bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			return fs.append(f, bufferSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void concat(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.Path
			[] psrcs)
		{
			fs.concat(f, psrcs);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt)
		{
			return fs.create(f, permission, flags, bufferSize, replication, blockSize, progress
				, checksumOpt);
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return fs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize
				, progress);
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		public override bool setReplication(org.apache.hadoop.fs.Path src, short replication
			)
		{
			return fs.setReplication(src, replication);
		}

		/// <summary>Renames Path src to Path dst.</summary>
		/// <remarks>
		/// Renames Path src to Path dst.  Can take place on local fs
		/// or remote DFS.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			return fs.rename(src, dst);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			return fs.truncate(f, newLength);
		}

		/// <summary>Delete a file</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			return fs.delete(f, recursive);
		}

		/// <summary>List files in a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.listStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.Path> listCorruptFileBlocks
			(org.apache.hadoop.fs.Path path)
		{
			return fs.listCorruptFileBlocks(path);
		}

		/// <summary>List files and its block locations in a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f)
		{
			return fs.listLocatedStatus(f);
		}

		/// <summary>Return a remote iterator for listing in a directory</summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path f)
		{
			return fs.listStatusIterator(f);
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return fs.getHomeDirectory();
		}

		/// <summary>Set the current working directory for the given file system.</summary>
		/// <remarks>
		/// Set the current working directory for the given file system. All relative
		/// paths will be resolved relative to it.
		/// </remarks>
		/// <param name="newDir"/>
		public override void setWorkingDirectory(org.apache.hadoop.fs.Path newDir)
		{
			fs.setWorkingDirectory(newDir);
		}

		/// <summary>Get the current working directory for the given file system</summary>
		/// <returns>the directory pathname</returns>
		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return fs.getWorkingDirectory();
		}

		protected internal override org.apache.hadoop.fs.Path getInitialWorkingDirectory(
			)
		{
			return fs.getInitialWorkingDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getStatus(org.apache.hadoop.fs.Path
			 p)
		{
			return fs.getStatus(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			return fs.mkdirs(f, permission);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, org.apache.hadoop.fs.Path src
			, org.apache.hadoop.fs.Path dst)
		{
			fs.copyFromLocalFile(delSrc, src, dst);
		}

		/// <summary>The src files are on the local disk.</summary>
		/// <remarks>
		/// The src files are on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, bool overwrite, org.apache.hadoop.fs.Path
			[] srcs, org.apache.hadoop.fs.Path dst)
		{
			fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, bool overwrite, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.Path dst)
		{
			fs.copyFromLocalFile(delSrc, overwrite, src, dst);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// delSrc indicates if the src will be removed or not.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, 
			org.apache.hadoop.fs.Path dst)
		{
			fs.copyToLocalFile(delSrc, src, dst);
		}

		/// <summary>Returns a local File that the user can write output to.</summary>
		/// <remarks>
		/// Returns a local File that the user can write output to.  The caller
		/// provides both the eventual FS target name and the local working
		/// file.  If the FS is local, we write directly into the target.  If
		/// the FS is remote, we write into the tmp local area.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path startLocalOutput(org.apache.hadoop.fs.Path
			 fsOutputFile, org.apache.hadoop.fs.Path tmpLocalFile)
		{
			return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
		}

		/// <summary>Called when we're all done writing to the target.</summary>
		/// <remarks>
		/// Called when we're all done writing to the target.  A local FS will
		/// do nothing, because we've written to exactly the right place.  A remote
		/// FS will copy the contents of tmpLocalFile to the correct target at
		/// fsOutputFile.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void completeLocalOutput(org.apache.hadoop.fs.Path fsOutputFile, 
			org.apache.hadoop.fs.Path tmpLocalFile)
		{
			fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
		}

		/// <summary>Return the total size of all files in the filesystem.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override long getUsed()
		{
			return fs.getUsed();
		}

		public override long getDefaultBlockSize()
		{
			return fs.getDefaultBlockSize();
		}

		public override short getDefaultReplication()
		{
			return fs.getDefaultReplication();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return fs.getServerDefaults();
		}

		// path variants delegate to underlying filesystem 
		public override long getDefaultBlockSize(org.apache.hadoop.fs.Path f)
		{
			return fs.getDefaultBlockSize(f);
		}

		public override short getDefaultReplication(org.apache.hadoop.fs.Path f)
		{
			return fs.getDefaultReplication(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getServerDefaults(f);
		}

		/// <summary>Get file status.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileStatus(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
			 mode)
		{
			fs.access(path, mode);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			fs.createSymlink(target, link, createParent);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileLinkStatus(f);
		}

		public override bool supportsSymlinks()
		{
			return fs.supportsSymlinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getLinkTarget(f);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.Path resolveLink(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.resolveLink(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileChecksum(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f, long length)
		{
			return fs.getFileChecksum(f, length);
		}

		public override void setVerifyChecksum(bool verifyChecksum)
		{
			fs.setVerifyChecksum(verifyChecksum);
		}

		public override void setWriteChecksum(bool writeChecksum)
		{
			fs.setWriteChecksum(writeChecksum);
		}

		public override org.apache.hadoop.conf.Configuration getConf()
		{
			return fs.getConf();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			base.close();
			fs.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path p, string username, string
			 groupname)
		{
			fs.setOwner(p, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path p, long mtime, long atime
			)
		{
			fs.setTimes(p, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path p, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			fs.setPermission(p, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.FSDataOutputStream primitiveCreate
			(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission absolutePermission
			, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, int bufferSize, short
			 replication, long blockSize, org.apache.hadoop.util.Progressable progress, org.apache.hadoop.fs.Options.ChecksumOpt
			 checksumOpt)
		{
			return fs.primitiveCreate(f, absolutePermission, flag, bufferSize, replication, blockSize
				, progress, checksumOpt);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool primitiveMkdir(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 abdolutePermission)
		{
			return fs.primitiveMkdir(f, abdolutePermission);
		}

		public override org.apache.hadoop.fs.FileSystem[] getChildFileSystems()
		{
			// FileSystem
			return new org.apache.hadoop.fs.FileSystem[] { fs };
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path createSnapshot(org.apache.hadoop.fs.Path
			 path, string snapshotName)
		{
			// FileSystem
			return fs.createSnapshot(path, snapshotName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void renameSnapshot(org.apache.hadoop.fs.Path path, string snapshotOldName
			, string snapshotNewName)
		{
			// FileSystem
			fs.renameSnapshot(path, snapshotOldName, snapshotNewName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteSnapshot(org.apache.hadoop.fs.Path path, string snapshotName
			)
		{
			// FileSystem
			fs.deleteSnapshot(path, snapshotName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			fs.modifyAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			fs.removeAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeDefaultAcl(org.apache.hadoop.fs.Path path)
		{
			fs.removeDefaultAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeAcl(org.apache.hadoop.fs.Path path)
		{
			fs.removeAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclSpec)
		{
			fs.setAcl(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
			 path)
		{
			return fs.getAclStatus(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value)
		{
			fs.setXAttr(path, name, value);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[]
			 value, java.util.EnumSet<org.apache.hadoop.fs.XAttrSetFlag> flag)
		{
			fs.setXAttr(path, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] getXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			return fs.getXAttr(path, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path)
		{
			return fs.getXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(
			org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<string> names)
		{
			return fs.getXAttrs(path, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path
			 path)
		{
			return fs.listXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void removeXAttr(org.apache.hadoop.fs.Path path, string name)
		{
			fs.removeXAttr(path, name);
		}
	}
}

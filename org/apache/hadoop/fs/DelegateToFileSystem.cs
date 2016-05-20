using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Implementation of AbstractFileSystem based on the existing implementation of
	/// <see cref="FileSystem"/>
	/// .
	/// </summary>
	public abstract class DelegateToFileSystem : org.apache.hadoop.fs.AbstractFileSystem
	{
		protected internal readonly org.apache.hadoop.fs.FileSystem fsImpl;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		protected internal DelegateToFileSystem(java.net.URI theUri, org.apache.hadoop.fs.FileSystem
			 theFsImpl, org.apache.hadoop.conf.Configuration conf, string supportedScheme, bool
			 authorityRequired)
			: base(theUri, supportedScheme, authorityRequired, getDefaultPortIfDefined(theFsImpl
				))
		{
			fsImpl = theFsImpl;
			fsImpl.initialize(theUri, conf);
			fsImpl.statistics = getStatistics();
		}

		/// <summary>Returns the default port if the file system defines one.</summary>
		/// <remarks>
		/// Returns the default port if the file system defines one.
		/// <see cref="FileSystem.getDefaultPort()"/>
		/// returns 0 to indicate the default port
		/// is undefined.  However, the logic that consumes this value expects to
		/// receive -1 to indicate the port is undefined, which agrees with the
		/// contract of
		/// <see cref="java.net.URI.getPort()"/>
		/// .
		/// </remarks>
		/// <param name="theFsImpl">file system to check for default port</param>
		/// <returns>default port, or -1 if default port is undefined</returns>
		private static int getDefaultPortIfDefined(org.apache.hadoop.fs.FileSystem theFsImpl
			)
		{
			int defaultPort = theFsImpl.getDefaultPort();
			return defaultPort != 0 ? defaultPort : -1;
		}

		public override org.apache.hadoop.fs.Path getInitialWorkingDirectory()
		{
			return fsImpl.getInitialWorkingDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
			)
		{
			// call to primitiveCreate
			checkPath(f);
			// Default impl assumes that permissions do not matter
			// calling the regular create is good enough.
			// FSs that implement permissions should override this.
			if (!createParent)
			{
				// parent must exist.
				// since this.create makes parent dirs automatically
				// we must throw exception if parent does not exist.
				org.apache.hadoop.fs.FileStatus stat = getFileStatus(f.getParent());
				if (stat == null)
				{
					throw new java.io.FileNotFoundException("Missing parent:" + f);
				}
				if (!stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.ParentNotDirectoryException("parent is not a dir:"
						 + f);
				}
			}
			// parent does exist - go ahead with create of file.
			return fsImpl.primitiveCreate(f, absolutePermission, flag, bufferSize, replication
				, blockSize, progress, checksumOpt);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			checkPath(f);
			return fsImpl.delete(f, recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			checkPath(f);
			return fsImpl.getFileBlockLocations(f, start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return fsImpl.getFileChecksum(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return fsImpl.getFileStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.FileStatus status = fsImpl.getFileLinkStatus(f);
			// FileSystem#getFileLinkStatus qualifies the link target
			// AbstractFileSystem needs to return it plain since it's qualified
			// in FileContext, so re-get and set the plain target
			if (status.isSymlink())
			{
				status.setSymlink(fsImpl.getLinkTarget(f));
			}
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getFsStatus()
		{
			return fsImpl.getStatus();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getFsStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fsImpl.getStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return fsImpl.getServerDefaults();
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return fsImpl.getHomeDirectory();
		}

		public override int getUriDefaultPort()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			checkPath(f);
			return fsImpl.listStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent)
		{
			// call to primitiveMkdir
			checkPath(dir);
			fsImpl.primitiveMkdir(dir, permission, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			checkPath(f);
			return fsImpl.open(f, bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			checkPath(f);
			return fsImpl.truncate(f, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			// call to rename
			checkPath(src);
			checkPath(dst);
			fsImpl.rename(src, dst, org.apache.hadoop.fs.Options.Rename.NONE);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			checkPath(f);
			fsImpl.setOwner(f, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			checkPath(f);
			fsImpl.setPermission(f, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			checkPath(f);
			return fsImpl.setReplication(f, replication);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			checkPath(f);
			fsImpl.setTimes(f, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setVerifyChecksum(bool verifyChecksum)
		{
			fsImpl.setVerifyChecksum(verifyChecksum);
		}

		public override bool supportsSymlinks()
		{
			return fsImpl.supportsSymlinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			fsImpl.createSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return fsImpl.getLinkTarget(f);
		}

		public override string getCanonicalServiceName()
		{
			//AbstractFileSystem
			return fsImpl.getCanonicalServiceName();
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.security.token.Token
			<object>> getDelegationTokens(string renewer)
		{
			//AbstractFileSystem
			return java.util.Arrays.asList(fsImpl.addDelegationTokens(renewer, null));
		}
	}
}

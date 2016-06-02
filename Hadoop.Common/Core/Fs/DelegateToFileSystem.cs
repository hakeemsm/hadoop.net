using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Implementation of AbstractFileSystem based on the existing implementation of
	/// <see cref="FileSystem"/>
	/// .
	/// </summary>
	public abstract class DelegateToFileSystem : AbstractFileSystem
	{
		protected internal readonly FileSystem fsImpl;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal DelegateToFileSystem(URI theUri, FileSystem theFsImpl, Configuration
			 conf, string supportedScheme, bool authorityRequired)
			: base(theUri, supportedScheme, authorityRequired, GetDefaultPortIfDefined(theFsImpl
				))
		{
			fsImpl = theFsImpl;
			fsImpl.Initialize(theUri, conf);
			fsImpl.statistics = GetStatistics();
		}

		/// <summary>Returns the default port if the file system defines one.</summary>
		/// <remarks>
		/// Returns the default port if the file system defines one.
		/// <see cref="FileSystem.GetDefaultPort()"/>
		/// returns 0 to indicate the default port
		/// is undefined.  However, the logic that consumes this value expects to
		/// receive -1 to indicate the port is undefined, which agrees with the
		/// contract of
		/// <see cref="Sharpen.URI.GetPort()"/>
		/// .
		/// </remarks>
		/// <param name="theFsImpl">file system to check for default port</param>
		/// <returns>default port, or -1 if default port is undefined</returns>
		private static int GetDefaultPortIfDefined(FileSystem theFsImpl)
		{
			int defaultPort = theFsImpl.GetDefaultPort();
			return defaultPort != 0 ? defaultPort : -1;
		}

		public override Path GetInitialWorkingDirectory()
		{
			return fsImpl.GetInitialWorkingDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
		{
			// call to primitiveCreate
			CheckPath(f);
			// Default impl assumes that permissions do not matter
			// calling the regular create is good enough.
			// FSs that implement permissions should override this.
			if (!createParent)
			{
				// parent must exist.
				// since this.create makes parent dirs automatically
				// we must throw exception if parent does not exist.
				FileStatus stat = GetFileStatus(f.GetParent());
				if (stat == null)
				{
					throw new FileNotFoundException("Missing parent:" + f);
				}
				if (!stat.IsDirectory())
				{
					throw new ParentNotDirectoryException("parent is not a dir:" + f);
				}
			}
			// parent does exist - go ahead with create of file.
			return fsImpl.PrimitiveCreate(f, absolutePermission, flag, bufferSize, replication
				, blockSize, progress, checksumOpt);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			CheckPath(f);
			return fsImpl.Delete(f, recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			)
		{
			CheckPath(f);
			return fsImpl.GetFileBlockLocations(f, start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			CheckPath(f);
			return fsImpl.GetFileChecksum(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			CheckPath(f);
			return fsImpl.GetFileStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			FileStatus status = fsImpl.GetFileLinkStatus(f);
			// FileSystem#getFileLinkStatus qualifies the link target
			// AbstractFileSystem needs to return it plain since it's qualified
			// in FileContext, so re-get and set the plain target
			if (status.IsSymlink())
			{
				status.SetSymlink(fsImpl.GetLinkTarget(f));
			}
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetFsStatus()
		{
			return fsImpl.GetStatus();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetFsStatus(Path f)
		{
			return fsImpl.GetStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return fsImpl.GetServerDefaults();
		}

		public override Path GetHomeDirectory()
		{
			return fsImpl.GetHomeDirectory();
		}

		public override int GetUriDefaultPort()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			CheckPath(f);
			return fsImpl.ListStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			// call to primitiveMkdir
			CheckPath(dir);
			fsImpl.PrimitiveMkdir(dir, permission, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			CheckPath(f);
			return fsImpl.Open(f, bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			CheckPath(f);
			return fsImpl.Truncate(f, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RenameInternal(Path src, Path dst)
		{
			// call to rename
			CheckPath(src);
			CheckPath(dst);
			fsImpl.Rename(src, dst, Options.Rename.None);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			CheckPath(f);
			fsImpl.SetOwner(f, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			CheckPath(f);
			fsImpl.SetPermission(f, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path f, short replication)
		{
			CheckPath(f);
			return fsImpl.SetReplication(f, replication);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			CheckPath(f);
			fsImpl.SetTimes(f, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			fsImpl.SetVerifyChecksum(verifyChecksum);
		}

		public override bool SupportsSymlinks()
		{
			return fsImpl.SupportsSymlinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			fsImpl.CreateSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return fsImpl.GetLinkTarget(f);
		}

		public override string GetCanonicalServiceName()
		{
			//AbstractFileSystem
			return fsImpl.GetCanonicalServiceName();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			(string renewer)
		{
			//AbstractFileSystem
			return Arrays.AsList(fsImpl.AddDelegationTokens(renewer, null));
		}
	}
}

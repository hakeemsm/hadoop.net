using System.Collections.Generic;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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
	public abstract class FilterFs : AbstractFileSystem
	{
		private readonly AbstractFileSystem myFs;

		/*Evolving for a release,to be changed to Stable */
		protected internal virtual AbstractFileSystem GetMyFs()
		{
			return myFs;
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal FilterFs(AbstractFileSystem fs)
			: base(fs.GetUri(), fs.GetUri().GetScheme(), fs.GetUri().GetAuthority() != null, 
				fs.GetUriDefaultPort())
		{
			myFs = fs;
		}

		public override FileSystem.Statistics GetStatistics()
		{
			return myFs.GetStatistics();
		}

		public override Path MakeQualified(Path path)
		{
			return myFs.MakeQualified(path);
		}

		public override Path GetInitialWorkingDirectory()
		{
			return myFs.GetInitialWorkingDirectory();
		}

		public override Path GetHomeDirectory()
		{
			return myFs.GetHomeDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
		{
			CheckPath(f);
			return myFs.CreateInternal(f, flag, absolutePermission, bufferSize, replication, 
				blockSize, progress, checksumOpt, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool Delete(Path f, bool recursive)
		{
			CheckPath(f);
			return myFs.Delete(f, recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			)
		{
			CheckPath(f);
			return myFs.GetFileBlockLocations(f, start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			CheckPath(f);
			return myFs.GetFileChecksum(f);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			CheckPath(f);
			return myFs.GetFileStatus(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			CheckPath(path);
			myFs.Access(path, mode);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			CheckPath(f);
			return myFs.GetFileLinkStatus(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetFsStatus(Path f)
		{
			return myFs.GetFsStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetFsStatus()
		{
			return myFs.GetFsStatus();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return myFs.GetServerDefaults();
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path p)
		{
			return myFs.ResolvePath(p);
		}

		public override int GetUriDefaultPort()
		{
			return myFs.GetUriDefaultPort();
		}

		public override URI GetUri()
		{
			return myFs.GetUri();
		}

		public override void CheckPath(Path path)
		{
			myFs.CheckPath(path);
		}

		public override string GetUriPath(Path p)
		{
			return myFs.GetUriPath(p);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			CheckPath(f);
			return myFs.ListStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<Path> ListCorruptFileBlocks(Path path)
		{
			return myFs.ListCorruptFileBlocks(path);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			CheckPath(dir);
			myFs.Mkdir(dir, permission, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f)
		{
			CheckPath(f);
			return myFs.Open(f);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			CheckPath(f);
			return myFs.Open(f, bufferSize);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			CheckPath(f);
			return myFs.Truncate(f, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst)
		{
			CheckPath(src);
			CheckPath(dst);
			myFs.Rename(src, dst, Options.Rename.None);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void RenameInternal(Path src, Path dst, bool overwrite)
		{
			myFs.RenameInternal(src, dst, overwrite);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			CheckPath(f);
			myFs.SetOwner(f, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			CheckPath(f);
			myFs.SetPermission(f, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool SetReplication(Path f, short replication)
		{
			CheckPath(f);
			return myFs.SetReplication(f, replication);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			CheckPath(f);
			myFs.SetTimes(f, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			myFs.SetVerifyChecksum(verifyChecksum);
		}

		public override bool SupportsSymlinks()
		{
			return myFs.SupportsSymlinks();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			myFs.CreateSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return myFs.GetLinkTarget(f);
		}

		public override string GetCanonicalServiceName()
		{
			// AbstractFileSystem
			return myFs.GetCanonicalServiceName();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			(string renewer)
		{
			// AbstractFileSystem
			return myFs.GetDelegationTokens(renewer);
		}

		public override bool IsValidName(string src)
		{
			return myFs.IsValidName(src);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			myFs.ModifyAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			myFs.RemoveAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			myFs.RemoveDefaultAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			myFs.RemoveAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			myFs.SetAcl(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			return myFs.GetAclStatus(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value)
		{
			myFs.SetXAttr(path, name, value);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			myFs.SetXAttr(path, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			return myFs.GetXAttr(path, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			return myFs.GetXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			return myFs.GetXAttrs(path, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			return myFs.ListXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			myFs.RemoveXAttr(path, name);
		}
	}
}

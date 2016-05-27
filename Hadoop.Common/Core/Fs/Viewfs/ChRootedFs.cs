using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
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
	internal class ChRootedFs : AbstractFileSystem
	{
		private readonly AbstractFileSystem myFs;

		private readonly URI myUri;

		private readonly Path chRootPathPart;

		private readonly string chRootPathPartString;

		/*Evolving for a release,to be changed to Stable */
		// the base file system whose root is changed
		// the base URI + the chroot
		// the root below the root of the base
		protected internal virtual AbstractFileSystem GetMyFs()
		{
			return myFs;
		}

		/// <param name="path"/>
		/// <returns>return full path including the chroot</returns>
		protected internal virtual Path FullPath(Path path)
		{
			base.CheckPath(path);
			return new Path((chRootPathPart.IsRoot() ? string.Empty : chRootPathPartString) +
				 path.ToUri().GetPath());
		}

		public override bool IsValidName(string src)
		{
			return myFs.IsValidName(FullPath(new Path(src)).ToUri().ToString());
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		public ChRootedFs(AbstractFileSystem fs, Path theRoot)
			: base(fs.GetUri(), fs.GetUri().GetScheme(), fs.GetUri().GetAuthority() != null, 
				fs.GetUriDefaultPort())
		{
			myFs = fs;
			myFs.CheckPath(theRoot);
			chRootPathPart = new Path(myFs.GetUriPath(theRoot));
			chRootPathPartString = chRootPathPart.ToUri().GetPath();
			/*
			* We are making URI include the chrootedPath: e.g. file:///chrootedPath.
			* This is questionable since Path#makeQualified(uri, path) ignores
			* the pathPart of a uri. Since this class is internal we can ignore
			* this issue but if we were to make it external then this needs
			* to be resolved.
			*/
			// Handle the two cases:
			//              scheme:/// and scheme://authority/
			myUri = new URI(myFs.GetUri().ToString() + (myFs.GetUri().GetAuthority() == null ? 
				string.Empty : Path.Separator) + Sharpen.Runtime.Substring(chRootPathPart.ToUri(
				).GetPath(), 1));
			base.CheckPath(theRoot);
		}

		public override URI GetUri()
		{
			return myUri;
		}

		/// <summary>Strip out the root from the path.</summary>
		/// <param name="p">- fully qualified path p</param>
		/// <returns>-  the remaining path  without the begining /</returns>
		public virtual string StripOutRoot(Path p)
		{
			try
			{
				CheckPath(p);
			}
			catch (ArgumentException)
			{
				throw new RuntimeException("Internal Error - path " + p + " should have been with URI"
					 + myUri);
			}
			string pathPart = p.ToUri().GetPath();
			return (pathPart.Length == chRootPathPartString.Length) ? string.Empty : Sharpen.Runtime.Substring
				(pathPart, chRootPathPartString.Length + (chRootPathPart.IsRoot() ? 0 : 1));
		}

		public override Path GetHomeDirectory()
		{
			return myFs.GetHomeDirectory();
		}

		public override Path GetInitialWorkingDirectory()
		{
			/*
			* 3 choices here: return null or / or strip out the root out of myFs's
			*  inital wd.
			* Only reasonable choice for initialWd for chrooted fds is null
			*/
			return null;
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		public virtual Path GetResolvedQualifiedPath(Path f)
		{
			return myFs.MakeQualified(new Path(chRootPathPartString + f.ToUri().ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
		{
			return myFs.CreateInternal(FullPath(f), flag, absolutePermission, bufferSize, replication
				, blockSize, progress, checksumOpt, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool Delete(Path f, bool recursive)
		{
			return myFs.Delete(FullPath(f), recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			)
		{
			return myFs.GetFileBlockLocations(FullPath(f), start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			return myFs.GetFileChecksum(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			return myFs.GetFileStatus(FullPath(f));
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			myFs.Access(FullPath(path), mode);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			return myFs.GetFileLinkStatus(FullPath(f));
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

		public override int GetUriDefaultPort()
		{
			return myFs.GetUriDefaultPort();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			return myFs.ListStatus(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			myFs.Mkdir(FullPath(dir), permission, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			return myFs.Open(FullPath(f), bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool Truncate(Path f, long newLength)
		{
			return myFs.Truncate(FullPath(f), newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst)
		{
			// note fullPath will check that paths are relative to this FileSystem.
			// Hence both are in same file system and a rename is valid
			myFs.RenameInternal(FullPath(src), FullPath(dst));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst, bool overwrite)
		{
			// note fullPath will check that paths are relative to this FileSystem.
			// Hence both are in same file system and a rename is valid
			myFs.RenameInternal(FullPath(src), FullPath(dst), overwrite);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			myFs.SetOwner(FullPath(f), username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			myFs.SetPermission(FullPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool SetReplication(Path f, short replication)
		{
			return myFs.SetReplication(FullPath(f), replication);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			myFs.SetTimes(FullPath(f), mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			myFs.ModifyAclEntries(FullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			myFs.RemoveAclEntries(FullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			myFs.RemoveDefaultAcl(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			myFs.RemoveAcl(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			myFs.SetAcl(FullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			return myFs.GetAclStatus(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			myFs.SetXAttr(FullPath(path), name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			return myFs.GetXAttr(FullPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			return myFs.GetXAttrs(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			return myFs.GetXAttrs(FullPath(path), names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			return myFs.ListXAttrs(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			myFs.RemoveXAttr(FullPath(path), name);
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
			/*
			* We leave the link alone:
			* If qualified or link relative then of course it is okay.
			* If absolute (ie / relative) then the link has to be resolved
			* relative to the changed root.
			*/
			myFs.CreateSymlink(FullPath(target), link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return myFs.GetLinkTarget(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			(string renewer)
		{
			return myFs.GetDelegationTokens(renewer);
		}
	}
}

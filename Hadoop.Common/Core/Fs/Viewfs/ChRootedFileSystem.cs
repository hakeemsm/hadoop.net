using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
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
	internal class ChRootedFileSystem : FilterFileSystem
	{
		private readonly URI myUri;

		private readonly Path chRootPathPart;

		private readonly string chRootPathPartString;

		private Path workingDir;

		/*Evolving for a release,to be changed to Stable */
		// the base URI + the chRoot
		// the root below the root of the base
		protected internal virtual FileSystem GetMyFs()
		{
			return GetRawFileSystem();
		}

		/// <param name="path"/>
		/// <returns>full path including the chroot</returns>
		protected internal virtual Path FullPath(Path path)
		{
			base.CheckPath(path);
			return path.IsAbsolute() ? new Path((chRootPathPart.IsRoot() ? string.Empty : chRootPathPartString
				) + path.ToUri().GetPath()) : new Path(chRootPathPartString + workingDir.ToUri()
				.GetPath(), path);
		}

		/// <summary>Constructor</summary>
		/// <param name="uri">base file system</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"></exception>
		public ChRootedFileSystem(URI uri, Configuration conf)
			: base(FileSystem.Get(uri, conf))
		{
			string pathString = uri.GetPath();
			if (pathString.IsEmpty())
			{
				pathString = "/";
			}
			chRootPathPart = new Path(pathString);
			chRootPathPartString = chRootPathPart.ToUri().GetPath();
			myUri = uri;
			workingDir = GetHomeDirectory();
		}

		// We don't use the wd of the myFs
		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="name">
		/// a uri whose authority section names the host, port, etc.
		/// for this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI name, Configuration conf)
		{
			base.Initialize(name, conf);
			SetConf(conf);
		}

		public override URI GetUri()
		{
			return myUri;
		}

		/// <summary>Strip out the root from the path.</summary>
		/// <param name="p">- fully qualified path p</param>
		/// <returns>-  the remaining path  without the begining /</returns>
		/// <exception cref="System.IO.IOException">if the p is not prefixed with root</exception>
		internal virtual string StripOutRoot(Path p)
		{
			try
			{
				CheckPath(p);
			}
			catch (ArgumentException)
			{
				throw new IOException("Internal Error - path " + p + " should have been with URI: "
					 + myUri);
			}
			string pathPart = p.ToUri().GetPath();
			return (pathPart.Length == chRootPathPartString.Length) ? string.Empty : Sharpen.Runtime.Substring
				(pathPart, chRootPathPartString.Length + (chRootPathPart.IsRoot() ? 0 : 1));
		}

		protected internal override Path GetInitialWorkingDirectory()
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

		/// <exception cref="System.IO.FileNotFoundException"/>
		public virtual Path GetResolvedQualifiedPath(Path f)
		{
			return MakeQualified(new Path(chRootPathPartString + f.ToUri().ToString()));
		}

		public override Path GetWorkingDirectory()
		{
			return workingDir;
		}

		public override void SetWorkingDirectory(Path new_dir)
		{
			workingDir = new_dir.IsAbsolute() ? new_dir : new Path(workingDir, new_dir);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			return base.Create(FullPath(f), permission, overwrite, bufferSize, replication, blockSize
				, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
			Progressable progress)
		{
			return base.CreateNonRecursive(FullPath(f), permission, flags, bufferSize, replication
				, blockSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			return base.Delete(FullPath(f), recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f)
		{
			return Delete(f, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(FileStatus fs, long start, 
			long len)
		{
			return base.GetFileBlockLocations(new ViewFsFileStatus(fs, FullPath(fs.GetPath())
				), start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			return base.GetFileChecksum(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			return base.GetFileStatus(FullPath(f));
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			base.Access(FullPath(path), mode);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetStatus(Path p)
		{
			return base.GetStatus(FullPath(p));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			return base.ListStatus(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path f)
		{
			return base.ListLocatedStatus(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			return base.Mkdirs(FullPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			return base.Open(FullPath(f), bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			return base.Append(FullPath(f), bufferSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			// note fullPath will check that paths are relative to this FileSystem.
			// Hence both are in same file system and a rename is valid
			return base.Rename(FullPath(src), FullPath(dst));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			base.SetOwner(FullPath(f), username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			base.SetPermission(FullPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path f, short replication)
		{
			return base.SetReplication(FullPath(f), replication);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			base.SetTimes(FullPath(f), mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			base.ModifyAclEntries(FullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			base.RemoveAclEntries(FullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			base.RemoveDefaultAcl(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			base.RemoveAcl(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			base.SetAcl(FullPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			return base.GetAclStatus(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			base.SetXAttr(FullPath(path), name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			return base.GetXAttr(FullPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			return base.GetXAttrs(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			return base.GetXAttrs(FullPath(path), names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			return base.ListXAttrs(FullPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			base.RemoveXAttr(FullPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path p)
		{
			return base.ResolvePath(FullPath(p));
		}

		/// <exception cref="System.IO.IOException"/>
		public override ContentSummary GetContentSummary(Path f)
		{
			return fs.GetContentSummary(FullPath(f));
		}

		private static Path rootPath = new Path(Path.Separator);

		public override long GetDefaultBlockSize()
		{
			return GetDefaultBlockSize(FullPath(rootPath));
		}

		public override long GetDefaultBlockSize(Path f)
		{
			return base.GetDefaultBlockSize(FullPath(f));
		}

		public override short GetDefaultReplication()
		{
			return GetDefaultReplication(FullPath(rootPath));
		}

		public override short GetDefaultReplication(Path f)
		{
			return base.GetDefaultReplication(FullPath(f));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return GetServerDefaults(FullPath(rootPath));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults(Path f)
		{
			return base.GetServerDefaults(FullPath(f));
		}
	}
}

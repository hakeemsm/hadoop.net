using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// ViewFileSystem (extends the FileSystem interface) implements a client-side
	/// mount table.
	/// </summary>
	/// <remarks>
	/// ViewFileSystem (extends the FileSystem interface) implements a client-side
	/// mount table. Its spec and implementation is identical to
	/// <see cref="ViewFs"/>
	/// .
	/// </remarks>
	public class ViewFileSystem : FileSystem
	{
		private static readonly Path RootPath = new Path(Path.Separator);

		/*Evolving for a release,to be changed to Stable */
		internal static AccessControlException ReadOnlyMountTable(string operation, string
			 p)
		{
			return new AccessControlException("InternalDir of ViewFileSystem is readonly; operation="
				 + operation + "Path=" + p);
		}

		internal static AccessControlException ReadOnlyMountTable(string operation, Path 
			p)
		{
			return ReadOnlyMountTable(operation, p.ToString());
		}

		public class MountPoint
		{
			private Path src;

			private URI[] targets;

			internal MountPoint(Path srcPath, URI[] targetURIs)
			{
				// the src of the mount
				//  target of the mount; Multiple targets imply mergeMount
				src = srcPath;
				targets = targetURIs;
			}

			internal virtual Path GetSrc()
			{
				return src;
			}

			internal virtual URI[] GetTargets()
			{
				return targets;
			}
		}

		internal readonly long creationTime;

		internal readonly UserGroupInformation ugi;

		internal URI myUri;

		private Path workingDir;

		internal Configuration config;

		internal InodeTree<FileSystem> fsState;

		internal Path homeDir = null;

		// of the the mount table
		// the user/group of user who created mtable
		// the fs state; ie the mount table
		/// <summary>Make the path Absolute and get the path-part of a pathname.</summary>
		/// <remarks>
		/// Make the path Absolute and get the path-part of a pathname.
		/// Checks that URI matches this file system
		/// and that the path-part is a valid name.
		/// </remarks>
		/// <param name="p">path</param>
		/// <returns>path-part of the Path p</returns>
		private string GetUriPath(Path p)
		{
			CheckPath(p);
			return MakeAbsolute(p).ToUri().GetPath();
		}

		private Path MakeAbsolute(Path f)
		{
			return f.IsAbsolute() ? f : new Path(workingDir, f);
		}

		/// <summary>
		/// This is the  constructor with the signature needed by
		/// <see cref="Org.Apache.Hadoop.FS.FileSystem.CreateFileSystem(Sharpen.URI, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// After this constructor is called initialize() is called.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		public ViewFileSystem()
		{
			ugi = UserGroupInformation.GetCurrentUser();
			creationTime = Time.Now();
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>viewfs</code></returns>
		public override string GetScheme()
		{
			return "viewfs";
		}

		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="theUri">
		/// a uri whose authority section names the host, port, etc. for
		/// this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI theUri, Configuration conf)
		{
			base.Initialize(theUri, conf);
			SetConf(conf);
			config = conf;
			// Now build  client side view (i.e. client side mount table) from config.
			string authority = theUri.GetAuthority();
			try
			{
				myUri = new URI(FsConstants.ViewfsScheme, authority, "/", null, null);
				fsState = new _InodeTree_167(this, conf, authority);
				// return MergeFs.createMergeFs(mergeFsURIList, config);
				workingDir = this.GetHomeDirectory();
			}
			catch (URISyntaxException)
			{
				throw new IOException("URISyntax exception: " + theUri);
			}
		}

		private sealed class _InodeTree_167 : InodeTree<FileSystem>
		{
			public _InodeTree_167(ViewFileSystem _enclosing, Configuration baseArg1, string baseArg2
				)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			/// <exception cref="System.IO.IOException"/>
			protected internal override FileSystem GetTargetFileSystem(URI uri)
			{
				return new ChRootedFileSystem(uri, this._enclosing.config);
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			protected internal override FileSystem GetTargetFileSystem(InodeTree.INodeDir<FileSystem
				> dir)
			{
				return new ViewFileSystem.InternalDirOfViewFs(dir, this._enclosing.creationTime, 
					this._enclosing.ugi, this._enclosing.myUri);
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
			protected internal override FileSystem GetTargetFileSystem(URI[] mergeFsURIList)
			{
				throw new UnsupportedFileSystemException("mergefs not implemented");
			}

			private readonly ViewFileSystem _enclosing;
		}

		/// <summary>Convenience Constructor for apps to call directly</summary>
		/// <param name="theUri">which must be that of ViewFileSystem</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		internal ViewFileSystem(URI theUri, Configuration conf)
			: this()
		{
			Initialize(theUri, conf);
		}

		/// <summary>Convenience Constructor for apps to call directly</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public ViewFileSystem(Configuration conf)
			: this(FsConstants.ViewfsUri, conf)
		{
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		public virtual Path GetTrashCanLocation(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.IsInternalDir() ? null : res.targetFileSystem.GetHomeDirectory();
		}

		public override URI GetUri()
		{
			return myUri;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res;
			res = fsState.Resolve(GetUriPath(f), true);
			if (res.IsInternalDir())
			{
				return f;
			}
			return res.targetFileSystem.ResolvePath(res.remainingPath);
		}

		public override Path GetHomeDirectory()
		{
			if (homeDir == null)
			{
				string @base = fsState.GetHomeDirPrefixValue();
				if (@base == null)
				{
					@base = "/user";
				}
				homeDir = (@base.Equals("/") ? this.MakeQualified(new Path(@base + ugi.GetShortUserName
					())) : this.MakeQualified(new Path(@base + "/" + ugi.GetShortUserName())));
			}
			return homeDir;
		}

		public override Path GetWorkingDirectory()
		{
			return workingDir;
		}

		public override void SetWorkingDirectory(Path new_dir)
		{
			GetUriPath(new_dir);
			// this validates the path
			workingDir = MakeAbsolute(new_dir);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.Append(res.remainingPath, bufferSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
			Progressable progress)
		{
			InodeTree.ResolveResult<FileSystem> res;
			try
			{
				res = fsState.Resolve(GetUriPath(f), false);
			}
			catch (FileNotFoundException)
			{
				throw ReadOnlyMountTable("create", f);
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			return res.targetFileSystem.CreateNonRecursive(res.remainingPath, permission, flags
				, bufferSize, replication, blockSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			InodeTree.ResolveResult<FileSystem> res;
			try
			{
				res = fsState.Resolve(GetUriPath(f), false);
			}
			catch (FileNotFoundException)
			{
				throw ReadOnlyMountTable("create", f);
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			return res.targetFileSystem.Create(res.remainingPath, permission, overwrite, bufferSize
				, replication, blockSize, progress);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			// If internal dir or target is a mount link (ie remainingPath is Slash)
			if (res.IsInternalDir() || res.remainingPath == InodeTree.SlashPath)
			{
				throw ReadOnlyMountTable("delete", f);
			}
			return res.targetFileSystem.Delete(res.remainingPath, recursive);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f)
		{
			return Delete(f, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(FileStatus fs, long start, 
			long len)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(fs.GetPath()
				), true);
			return res.targetFileSystem.GetFileBlockLocations(new ViewFsFileStatus(fs, res.remainingPath
				), start, len);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.GetFileChecksum(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		private static FileStatus FixFileStatus(FileStatus orig, Path qualified)
		{
			// FileStatus#getPath is a fully qualified path relative to the root of
			// target file system.
			// We need to change it to viewfs URI - relative to root of mount table.
			// The implementors of RawLocalFileSystem were trying to be very smart.
			// They implement FileStatus#getOwner lazily -- the object
			// returned is really a RawLocalFileSystem that expect the
			// FileStatus#getPath to be unchanged so that it can get owner when needed.
			// Hence we need to interpose a new ViewFileSystemFileStatus that
			// works around.
			if ("file".Equals(orig.GetPath().ToUri().GetScheme()))
			{
				orig = WrapLocalFileStatus(orig, qualified);
			}
			orig.SetPath(qualified);
			return orig;
		}

		private static FileStatus WrapLocalFileStatus(FileStatus orig, Path qualified)
		{
			return orig is LocatedFileStatus ? new ViewFsLocatedFileStatus((LocatedFileStatus
				)orig, qualified) : new ViewFsFileStatus(orig, qualified);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			FileStatus status = res.targetFileSystem.GetFileStatus(res.remainingPath);
			return FixFileStatus(status, this.MakeQualified(f));
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.Access(res.remainingPath, mode);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			FileStatus[] statusLst = res.targetFileSystem.ListStatus(res.remainingPath);
			if (!res.IsInternalDir())
			{
				// We need to change the name in the FileStatus as described in
				// {@link #getFileStatus }
				int i = 0;
				foreach (FileStatus status in statusLst)
				{
					statusLst[i++] = FixFileStatus(status, GetChrootedPath(res, status, f));
				}
			}
			return statusLst;
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path
			 f, PathFilter filter)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			RemoteIterator<LocatedFileStatus> statusIter = res.targetFileSystem.ListLocatedStatus
				(res.remainingPath);
			if (res.IsInternalDir())
			{
				return statusIter;
			}
			return new _RemoteIterator_422(this, statusIter, res, f);
		}

		private sealed class _RemoteIterator_422 : RemoteIterator<LocatedFileStatus>
		{
			public _RemoteIterator_422(ViewFileSystem _enclosing, RemoteIterator<LocatedFileStatus
				> statusIter, InodeTree.ResolveResult<FileSystem> res, Path f)
			{
				this._enclosing = _enclosing;
				this.statusIter = statusIter;
				this.res = res;
				this.f = f;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool HasNext()
			{
				return statusIter.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public LocatedFileStatus Next()
			{
				LocatedFileStatus status = statusIter.Next();
				return (LocatedFileStatus)ViewFileSystem.FixFileStatus(status, this._enclosing.GetChrootedPath
					(res, status, f));
			}

			private readonly ViewFileSystem _enclosing;

			private readonly RemoteIterator<LocatedFileStatus> statusIter;

			private readonly InodeTree.ResolveResult<FileSystem> res;

			private readonly Path f;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path GetChrootedPath(InodeTree.ResolveResult<FileSystem> res, FileStatus 
			status, Path f)
		{
			string suffix = ((ChRootedFileSystem)res.targetFileSystem).StripOutRoot(status.GetPath
				());
			return this.MakeQualified(suffix.Length == 0 ? f : new Path(res.resolvedPath, suffix
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path dir, FsPermission permission)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(dir), false);
			return res.targetFileSystem.Mkdirs(res.remainingPath, permission);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.Open(res.remainingPath, bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			// passing resolveLastComponet as false to catch renaming a mount point to 
			// itself. We need to catch this as an internal operation and fail.
			InodeTree.ResolveResult<FileSystem> resSrc = fsState.Resolve(GetUriPath(src), false
				);
			if (resSrc.IsInternalDir())
			{
				throw ReadOnlyMountTable("rename", src);
			}
			InodeTree.ResolveResult<FileSystem> resDst = fsState.Resolve(GetUriPath(dst), false
				);
			if (resDst.IsInternalDir())
			{
				throw ReadOnlyMountTable("rename", dst);
			}
			//
			// Alternate 3 : renames ONLY within the the same mount links.
			//
			if (resSrc.targetFileSystem != resDst.targetFileSystem)
			{
				throw new IOException("Renames across Mount points not supported");
			}
			return resSrc.targetFileSystem.Rename(resSrc.remainingPath, resDst.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.Truncate(f, newLength);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			res.targetFileSystem.SetOwner(res.remainingPath, username, groupname);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			res.targetFileSystem.SetPermission(res.remainingPath, permission);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path f, short replication)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.SetReplication(res.remainingPath, replication);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			res.targetFileSystem.SetTimes(res.remainingPath, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.ModifyAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.RemoveAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.RemoveDefaultAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.RemoveAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.SetAcl(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			return res.targetFileSystem.GetAclStatus(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.SetXAttr(res.remainingPath, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			return res.targetFileSystem.GetXAttr(res.remainingPath, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			return res.targetFileSystem.GetXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			return res.targetFileSystem.GetXAttrs(res.remainingPath, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			return res.targetFileSystem.ListXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(path), true);
			res.targetFileSystem.RemoveXAttr(res.remainingPath, name);
		}

		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			IList<InodeTree.MountPoint<FileSystem>> mountPoints = fsState.GetMountPoints();
			foreach (InodeTree.MountPoint<FileSystem> mount in mountPoints)
			{
				mount.target.targetFileSystem.SetVerifyChecksum(verifyChecksum);
			}
		}

		public override long GetDefaultBlockSize()
		{
			throw new NotInMountpointException("getDefaultBlockSize");
		}

		public override short GetDefaultReplication()
		{
			throw new NotInMountpointException("getDefaultReplication");
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			throw new NotInMountpointException("getServerDefaults");
		}

		public override long GetDefaultBlockSize(Path f)
		{
			try
			{
				InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
				return res.targetFileSystem.GetDefaultBlockSize(res.remainingPath);
			}
			catch (FileNotFoundException)
			{
				throw new NotInMountpointException(f, "getDefaultBlockSize");
			}
		}

		public override short GetDefaultReplication(Path f)
		{
			try
			{
				InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
				return res.targetFileSystem.GetDefaultReplication(res.remainingPath);
			}
			catch (FileNotFoundException)
			{
				throw new NotInMountpointException(f, "getDefaultReplication");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.GetServerDefaults(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override ContentSummary GetContentSummary(Path f)
		{
			InodeTree.ResolveResult<FileSystem> res = fsState.Resolve(GetUriPath(f), true);
			return res.targetFileSystem.GetContentSummary(res.remainingPath);
		}

		public override void SetWriteChecksum(bool writeChecksum)
		{
			IList<InodeTree.MountPoint<FileSystem>> mountPoints = fsState.GetMountPoints();
			foreach (InodeTree.MountPoint<FileSystem> mount in mountPoints)
			{
				mount.target.targetFileSystem.SetWriteChecksum(writeChecksum);
			}
		}

		public override FileSystem[] GetChildFileSystems()
		{
			IList<InodeTree.MountPoint<FileSystem>> mountPoints = fsState.GetMountPoints();
			ICollection<FileSystem> children = new HashSet<FileSystem>();
			foreach (InodeTree.MountPoint<FileSystem> mountPoint in mountPoints)
			{
				FileSystem targetFs = mountPoint.target.targetFileSystem;
				Sharpen.Collections.AddAll(children, Arrays.AsList(targetFs.GetChildFileSystems()
					));
			}
			return Sharpen.Collections.ToArray(children, new FileSystem[] {  });
		}

		public virtual ViewFileSystem.MountPoint[] GetMountPoints()
		{
			IList<InodeTree.MountPoint<FileSystem>> mountPoints = fsState.GetMountPoints();
			ViewFileSystem.MountPoint[] result = new ViewFileSystem.MountPoint[mountPoints.Count
				];
			for (int i = 0; i < mountPoints.Count; ++i)
			{
				result[i] = new ViewFileSystem.MountPoint(new Path(mountPoints[i].src), mountPoints
					[i].target.targetDirLinkList);
			}
			return result;
		}

		internal class InternalDirOfViewFs : FileSystem
		{
			internal readonly InodeTree.INodeDir<FileSystem> theInternalDir;

			internal readonly long creationTime;

			internal readonly UserGroupInformation ugi;

			internal readonly URI myUri;

			/// <exception cref="Sharpen.URISyntaxException"/>
			public InternalDirOfViewFs(InodeTree.INodeDir<FileSystem> dir, long cTime, UserGroupInformation
				 ugi, URI uri)
			{
				/*
				* An instance of this class represents an internal dir of the viewFs
				* that is internal dir of the mount table.
				* It is a read only mount tables and create, mkdir or delete operations
				* are not allowed.
				* If called on create or mkdir then this target is the parent of the
				* directory in which one is trying to create or mkdir; hence
				* in this case the path name passed in is the last component.
				* Otherwise this target is the end point of the path and hence
				* the path name passed in is null.
				*/
				// of the the mount table
				// the user/group of user who created mtable
				myUri = uri;
				try
				{
					Initialize(myUri, new Configuration());
				}
				catch (IOException)
				{
					throw new RuntimeException("Cannot occur");
				}
				theInternalDir = dir;
				creationTime = cTime;
				this.ugi = ugi;
			}

			/// <exception cref="System.IO.IOException"/>
			private static void CheckPathIsSlash(Path f)
			{
				if (f != InodeTree.SlashPath)
				{
					throw new IOException("Internal implementation error: expected file name to be /"
						);
				}
			}

			public override URI GetUri()
			{
				return myUri;
			}

			public override Path GetWorkingDirectory()
			{
				throw new RuntimeException("Internal impl error: getWorkingDir should not have been called"
					);
			}

			public override void SetWorkingDirectory(Path new_dir)
			{
				throw new RuntimeException("Internal impl error: getWorkingDir should not have been called"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
				)
			{
				throw ReadOnlyMountTable("append", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
				, int bufferSize, short replication, long blockSize, Progressable progress)
			{
				throw ReadOnlyMountTable("create", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path f, bool recursive)
			{
				CheckPathIsSlash(f);
				throw ReadOnlyMountTable("delete", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path f)
			{
				return Delete(f, true);
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override BlockLocation[] GetFileBlockLocations(FileStatus fs, long start, 
				long len)
			{
				CheckPathIsSlash(fs.GetPath());
				throw new FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FileChecksum GetFileChecksum(Path f)
			{
				CheckPathIsSlash(f);
				throw new FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				CheckPathIsSlash(f);
				return new FileStatus(0, true, 0, 0, creationTime, creationTime, Constants.Permission555
					, ugi.GetUserName(), ugi.GetGroupNames()[0], new Path(theInternalDir.fullPath).MakeQualified
					(myUri, RootPath));
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				CheckPathIsSlash(f);
				FileStatus[] result = new FileStatus[theInternalDir.children.Count];
				int i = 0;
				foreach (KeyValuePair<string, InodeTree.INode<FileSystem>> iEntry in theInternalDir
					.children)
				{
					InodeTree.INode<FileSystem> inode = iEntry.Value;
					if (inode is InodeTree.INodeLink)
					{
						InodeTree.INodeLink<FileSystem> link = (InodeTree.INodeLink<FileSystem>)inode;
						result[i++] = new FileStatus(0, false, 0, 0, creationTime, creationTime, Constants
							.Permission555, ugi.GetUserName(), ugi.GetGroupNames()[0], link.GetTargetLink(), 
							new Path(inode.fullPath).MakeQualified(myUri, null));
					}
					else
					{
						result[i++] = new FileStatus(0, true, 0, 0, creationTime, creationTime, Constants
							.Permission555, ugi.GetUserName(), ugi.GetGroupNames()[0], new Path(inode.fullPath
							).MakeQualified(myUri, null));
					}
				}
				return result;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
			public override bool Mkdirs(Path dir, FsPermission permission)
			{
				if (theInternalDir.isRoot && dir == null)
				{
					throw new FileAlreadyExistsException("/ already exits");
				}
				// Note dir starts with /
				if (theInternalDir.children.Contains(Sharpen.Runtime.Substring(dir.ToString(), 1)
					))
				{
					return true;
				}
				// this is the stupid semantics of FileSystem
				throw ReadOnlyMountTable("mkdirs", dir);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Open(Path f, int bufferSize)
			{
				CheckPathIsSlash(f);
				throw new FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool Rename(Path src, Path dst)
			{
				CheckPathIsSlash(src);
				CheckPathIsSlash(dst);
				throw ReadOnlyMountTable("rename", src);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Truncate(Path f, long newLength)
			{
				throw ReadOnlyMountTable("truncate", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void SetOwner(Path f, string username, string groupname)
			{
				CheckPathIsSlash(f);
				throw ReadOnlyMountTable("setOwner", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void SetPermission(Path f, FsPermission permission)
			{
				CheckPathIsSlash(f);
				throw ReadOnlyMountTable("setPermission", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool SetReplication(Path f, short replication)
			{
				CheckPathIsSlash(f);
				throw ReadOnlyMountTable("setReplication", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void SetTimes(Path f, long mtime, long atime)
			{
				CheckPathIsSlash(f);
				throw ReadOnlyMountTable("setTimes", f);
			}

			public override void SetVerifyChecksum(bool verifyChecksum)
			{
			}

			// Noop for viewfs
			/// <exception cref="System.IO.IOException"/>
			public override FsServerDefaults GetServerDefaults(Path f)
			{
				throw new NotInMountpointException(f, "getServerDefaults");
			}

			public override long GetDefaultBlockSize(Path f)
			{
				throw new NotInMountpointException(f, "getDefaultBlockSize");
			}

			public override short GetDefaultReplication(Path f)
			{
				throw new NotInMountpointException(f, "getDefaultReplication");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("modifyAclEntries", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("removeAclEntries", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveDefaultAcl(Path path)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("removeDefaultAcl", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveAcl(Path path)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("removeAcl", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetAcl(Path path, IList<AclEntry> aclSpec)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("setAcl", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override AclStatus GetAclStatus(Path path)
			{
				CheckPathIsSlash(path);
				return new AclStatus.Builder().Owner(ugi.GetUserName()).Group(ugi.GetGroupNames()
					[0]).AddEntries(AclUtil.GetMinimalAcl(Constants.Permission555)).StickyBit(false)
					.Build();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
				> flag)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("setXAttr", path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override byte[] GetXAttr(Path path, string name)
			{
				throw new NotInMountpointException(path, "getXAttr");
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, byte[]> GetXAttrs(Path path)
			{
				throw new NotInMountpointException(path, "getXAttrs");
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
				)
			{
				throw new NotInMountpointException(path, "getXAttrs");
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> ListXAttrs(Path path)
			{
				throw new NotInMountpointException(path, "listXAttrs");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveXAttr(Path path, string name)
			{
				CheckPathIsSlash(path);
				throw ReadOnlyMountTable("removeXAttr", path);
			}
		}
	}
}

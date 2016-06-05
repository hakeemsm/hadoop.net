using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Local;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// ViewFs (extends the AbstractFileSystem interface) implements a client-side
	/// mount table.
	/// </summary>
	/// <remarks>
	/// ViewFs (extends the AbstractFileSystem interface) implements a client-side
	/// mount table. The viewFs file system is implemented completely in memory on
	/// the client side. The client-side mount table allows a client to provide a
	/// customized view of a file system namespace that is composed from
	/// one or more individual file systems (a localFs or Hdfs, S3fs, etc).
	/// For example one could have a mount table that provides links such as
	/// <ul>
	/// <li>  /user          -&gt; hdfs://nnContainingUserDir/user
	/// <li>  /project/foo   -&gt; hdfs://nnProject1/projects/foo
	/// <li>  /project/bar   -&gt; hdfs://nnProject2/projects/bar
	/// <li>  /tmp           -&gt; hdfs://nnTmp/privateTmpForUserXXX
	/// </ul>
	/// ViewFs is specified with the following URI: <b>viewfs:///</b>
	/// <p>
	/// To use viewfs one would typically set the default file system in the
	/// config  (i.e. fs.default.name&lt; = viewfs:///) along with the
	/// mount table config variables as described below.
	/// <p>
	/// <b> ** Config variables to specify the mount table entries ** </b>
	/// <p>
	/// The file system is initialized from the standard Hadoop config through
	/// config variables.
	/// See
	/// <see cref="Org.Apache.Hadoop.FS.FsConstants"/>
	/// for URI and Scheme constants;
	/// See
	/// <see cref="Constants"/>
	/// for config var constants;
	/// see
	/// <see cref="ConfigUtil"/>
	/// for convenient lib.
	/// <p>
	/// All the mount table config entries for view fs are prefixed by
	/// <b>fs.viewfs.mounttable.</b>
	/// For example the above example can be specified with the following
	/// config variables:
	/// <ul>
	/// <li> fs.viewfs.mounttable.default.link./user=
	/// hdfs://nnContainingUserDir/user
	/// <li> fs.viewfs.mounttable.default.link./project/foo=
	/// hdfs://nnProject1/projects/foo
	/// <li> fs.viewfs.mounttable.default.link./project/bar=
	/// hdfs://nnProject2/projects/bar
	/// <li> fs.viewfs.mounttable.default.link./tmp=
	/// hdfs://nnTmp/privateTmpForUserXXX
	/// </ul>
	/// The default mount table (when no authority is specified) is
	/// from config variables prefixed by <b>fs.viewFs.mounttable.default </b>
	/// The authority component of a URI can be used to specify a different mount
	/// table. For example,
	/// <ul>
	/// <li>  viewfs://sanjayMountable/
	/// </ul>
	/// is initialized from fs.viewFs.mounttable.sanjayMountable.* config variables.
	/// <p>
	/// <b> **** Merge Mounts **** </b>(NOTE: merge mounts are not implemented yet.)
	/// <p>
	/// One can also use "MergeMounts" to merge several directories (this is
	/// sometimes  called union-mounts or junction-mounts in the literature.
	/// For example of the home directories are stored on say two file systems
	/// (because they do not fit on one) then one could specify a mount
	/// entry such as following merges two dirs:
	/// <ul>
	/// <li> /user -&gt; hdfs://nnUser1/user,hdfs://nnUser2/user
	/// </ul>
	/// Such a mergeLink can be specified with the following config var where ","
	/// is used as the separator for each of links to be merged:
	/// <ul>
	/// <li> fs.viewfs.mounttable.default.linkMerge./user=
	/// hdfs://nnUser1/user,hdfs://nnUser1/user
	/// </ul>
	/// A special case of the merge mount is where mount table's root is merged
	/// with the root (slash) of another file system:
	/// <ul>
	/// <li>    fs.viewfs.mounttable.default.linkMergeSlash=hdfs://nn99/
	/// </ul>
	/// In this cases the root of the mount table is merged with the root of
	/// <b>hdfs://nn99/ </b>
	/// </remarks>
	public class ViewFs : AbstractFileSystem
	{
		internal readonly long creationTime;

		internal readonly UserGroupInformation ugi;

		internal readonly Configuration config;

		internal InodeTree<AbstractFileSystem> fsState;

		internal Path homeDir = null;

		/*Evolving for a release,to be changed to Stable */
		// of the the mount table
		// the user/group of user who created mtable
		// the fs state; ie the mount table
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="URISyntaxException"/>
		public ViewFs(Configuration conf)
			: this(FsConstants.ViewfsUri, conf)
		{
		}

		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="Org.Apache.Hadoop.FS.AbstractFileSystem.CreateFileSystem(URI, Configuration)
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="theUri">which must be that of ViewFs</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="URISyntaxException"></exception>
		internal ViewFs(URI theUri, Configuration conf)
			: base(theUri, FsConstants.ViewfsScheme, false, -1)
		{
			creationTime = Time.Now();
			ugi = UserGroupInformation.GetCurrentUser();
			config = conf;
			// Now build  client side view (i.e. client side mount table) from config.
			string authority = theUri.GetAuthority();
			fsState = new _InodeTree_208(this, conf, authority);
		}

		private sealed class _InodeTree_208 : InodeTree<AbstractFileSystem>
		{
			public _InodeTree_208(ViewFs _enclosing, Configuration baseArg1, string baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="URISyntaxException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
			protected internal override AbstractFileSystem GetTargetFileSystem(URI uri)
			{
				string pathString = uri.GetPath();
				if (pathString.IsEmpty())
				{
					pathString = "/";
				}
				return new ChRootedFs(AbstractFileSystem.CreateFileSystem(uri, this._enclosing.config
					), new Path(pathString));
			}

			/// <exception cref="URISyntaxException"/>
			protected internal override AbstractFileSystem GetTargetFileSystem(InodeTree.INodeDir
				<AbstractFileSystem> dir)
			{
				return new ViewFs.InternalDirOfViewFs(dir, this._enclosing.creationTime, this._enclosing
					.ugi, this._enclosing.GetUri());
			}

			/// <exception cref="URISyntaxException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
			protected internal override AbstractFileSystem GetTargetFileSystem(URI[] mergeFsURIList
				)
			{
				throw new UnsupportedFileSystemException("mergefs not implemented yet");
			}

			private readonly ViewFs _enclosing;
		}

		// return MergeFs.createMergeFs(mergeFsURIList, config);
		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return LocalConfigKeys.GetServerDefaults();
		}

		public override int GetUriDefaultPort()
		{
			return -1;
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

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res;
			res = fsState.Resolve(GetUriPath(f), true);
			if (res.IsInternalDir())
			{
				return f;
			}
			return res.targetFileSystem.ResolvePath(res.remainingPath);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res;
			try
			{
				res = fsState.Resolve(GetUriPath(f), false);
			}
			catch (FileNotFoundException e)
			{
				if (createParent)
				{
					throw ReadOnlyMountTable("create", f);
				}
				else
				{
					throw;
				}
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			return res.targetFileSystem.CreateInternal(res.remainingPath, flag, absolutePermission
				, bufferSize, replication, blockSize, progress, checksumOpt, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			// If internal dir or target is a mount link (ie remainingPath is Slash)
			if (res.IsInternalDir() || res.remainingPath == InodeTree.SlashPath)
			{
				throw new AccessControlException("Cannot delete internal mount table directory: "
					 + f);
			}
			return res.targetFileSystem.Delete(res.remainingPath, recursive);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			return res.targetFileSystem.GetFileBlockLocations(res.remainingPath, start, len);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			return res.targetFileSystem.GetFileChecksum(res.remainingPath);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			//  FileStatus#getPath is a fully qualified path relative to the root of 
			// target file system.
			// We need to change it to viewfs URI - relative to root of mount table.
			// The implementors of RawLocalFileSystem were trying to be very smart.
			// They implement FileStatus#getOwener lazily -- the object
			// returned is really a RawLocalFileSystem that expect the
			// FileStatus#getPath to be unchanged so that it can get owner when needed.
			// Hence we need to interpose a new ViewFsFileStatus that works around.
			FileStatus status = res.targetFileSystem.GetFileStatus(res.remainingPath);
			return new ViewFsFileStatus(status, this.MakeQualified(f));
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.Access(res.remainingPath, mode);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				false);
			// do not follow mount link
			return res.targetFileSystem.GetFileLinkStatus(res.remainingPath);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetFsStatus()
		{
			return new FsStatus(0, 0, 0);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<FileStatus> ListStatusIterator(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			RemoteIterator<FileStatus> fsIter = res.targetFileSystem.ListStatusIterator(res.remainingPath
				);
			if (res.IsInternalDir())
			{
				return fsIter;
			}
			return new _RemoteIterator_391(this, fsIter, res, f);
		}

		private sealed class _RemoteIterator_391 : RemoteIterator<FileStatus>
		{
			public _RemoteIterator_391(ViewFs _enclosing, RemoteIterator<FileStatus> fsIter, 
				InodeTree.ResolveResult<AbstractFileSystem> res, Path f)
			{
				this._enclosing = _enclosing;
				this.fsIter = fsIter;
				this.res = res;
				this.f = f;
				{
					// Init
					this.myIter = fsIter;
					this.targetFs = (ChRootedFs)res.targetFileSystem;
				}
			}

			internal readonly RemoteIterator<FileStatus> myIter;

			internal readonly ChRootedFs targetFs;

			/// <exception cref="System.IO.IOException"/>
			public bool HasNext()
			{
				return this.myIter.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public FileStatus Next()
			{
				FileStatus status = this.myIter.Next();
				string suffix = this.targetFs.StripOutRoot(status.GetPath());
				return new ViewFsFileStatus(status, this._enclosing.MakeQualified(suffix.Length ==
					 0 ? f : new Path(res.resolvedPath, suffix)));
			}

			private readonly ViewFs _enclosing;

			private readonly RemoteIterator<FileStatus> fsIter;

			private readonly InodeTree.ResolveResult<AbstractFileSystem> res;

			private readonly Path f;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			FileStatus[] statusLst = res.targetFileSystem.ListStatus(res.remainingPath);
			if (!res.IsInternalDir())
			{
				// We need to change the name in the FileStatus as described in
				// {@link #getFileStatus }
				ChRootedFs targetFs;
				targetFs = (ChRootedFs)res.targetFileSystem;
				int i = 0;
				foreach (FileStatus status in statusLst)
				{
					string suffix = targetFs.StripOutRoot(status.GetPath());
					statusLst[i++] = new ViewFsFileStatus(status, this.MakeQualified(suffix.Length ==
						 0 ? f : new Path(res.resolvedPath, suffix)));
				}
			}
			return statusLst;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(dir)
				, false);
			res.targetFileSystem.Mkdir(res.remainingPath, permission, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			return res.targetFileSystem.Open(res.remainingPath, bufferSize);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			return res.targetFileSystem.Truncate(res.remainingPath, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst, bool overwrite)
		{
			// passing resolveLastComponet as false to catch renaming a mount point 
			// itself we need to catch this as an internal operation and fail.
			InodeTree.ResolveResult<AbstractFileSystem> resSrc = fsState.Resolve(GetUriPath(src
				), false);
			if (resSrc.IsInternalDir())
			{
				throw new AccessControlException("Cannot Rename within internal dirs of mount table: it is readOnly"
					);
			}
			InodeTree.ResolveResult<AbstractFileSystem> resDst = fsState.Resolve(GetUriPath(dst
				), false);
			if (resDst.IsInternalDir())
			{
				throw new AccessControlException("Cannot Rename within internal dirs of mount table: it is readOnly"
					);
			}
			//
			// Alternate 3 : renames ONLY within the the same mount links.
			//
			if (resSrc.targetFileSystem != resDst.targetFileSystem)
			{
				throw new IOException("Renames across Mount points not supported");
			}
			resSrc.targetFileSystem.RenameInternal(resSrc.remainingPath, resDst.remainingPath
				, overwrite);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void RenameInternal(Path src, Path dst)
		{
			RenameInternal(src, dst, false);
		}

		public override bool SupportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res;
			try
			{
				res = fsState.Resolve(GetUriPath(link), false);
			}
			catch (FileNotFoundException e)
			{
				if (createParent)
				{
					throw ReadOnlyMountTable("createSymlink", link);
				}
				else
				{
					throw;
				}
			}
			System.Diagnostics.Debug.Assert((res.remainingPath != null));
			res.targetFileSystem.CreateSymlink(target, res.remainingPath, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				false);
			// do not follow mount link
			return res.targetFileSystem.GetLinkTarget(res.remainingPath);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			res.targetFileSystem.SetOwner(res.remainingPath, username, groupname);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			res.targetFileSystem.SetPermission(res.remainingPath, permission);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path f, short replication)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			return res.targetFileSystem.SetReplication(res.remainingPath, replication);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(f), 
				true);
			res.targetFileSystem.SetTimes(res.remainingPath, mtime, atime);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetVerifyChecksum(bool verifyChecksum)
		{
		}

		// This is a file system level operations, however ViewFs 
		// points to many file systems. Noop for ViewFs. 
		public virtual ViewFs.MountPoint[] GetMountPoints()
		{
			IList<InodeTree.MountPoint<AbstractFileSystem>> mountPoints = fsState.GetMountPoints
				();
			ViewFs.MountPoint[] result = new ViewFs.MountPoint[mountPoints.Count];
			for (int i = 0; i < mountPoints.Count; ++i)
			{
				result[i] = new ViewFs.MountPoint(new Path(mountPoints[i].src), mountPoints[i].target
					.targetDirLinkList);
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			(string renewer)
		{
			IList<InodeTree.MountPoint<AbstractFileSystem>> mountPoints = fsState.GetMountPoints
				();
			int initialListSize = 0;
			foreach (InodeTree.MountPoint<AbstractFileSystem> im in mountPoints)
			{
				initialListSize += im.target.targetDirLinkList.Length;
			}
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> result = new AList<Org.Apache.Hadoop.Security.Token.Token
				<object>>(initialListSize);
			for (int i = 0; i < mountPoints.Count; ++i)
			{
				IList<Org.Apache.Hadoop.Security.Token.Token<object>> tokens = mountPoints[i].target
					.targetFileSystem.GetDelegationTokens(renewer);
				if (tokens != null)
				{
					Collections.AddAll(result, tokens);
				}
			}
			return result;
		}

		public override bool IsValidName(string src)
		{
			// Prefix validated at mount time and rest of path validated by mount target.
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.ModifyAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.RemoveAclEntries(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.RemoveDefaultAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.RemoveAcl(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.SetAcl(res.remainingPath, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			return res.targetFileSystem.GetAclStatus(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.SetXAttr(res.remainingPath, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			return res.targetFileSystem.GetXAttr(res.remainingPath, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			return res.targetFileSystem.GetXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			return res.targetFileSystem.GetXAttrs(res.remainingPath, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			return res.targetFileSystem.ListXAttrs(res.remainingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			InodeTree.ResolveResult<AbstractFileSystem> res = fsState.Resolve(GetUriPath(path
				), true);
			res.targetFileSystem.RemoveXAttr(res.remainingPath, name);
		}

		internal class InternalDirOfViewFs : AbstractFileSystem
		{
			internal readonly InodeTree.INodeDir<AbstractFileSystem> theInternalDir;

			internal readonly long creationTime;

			internal readonly UserGroupInformation ugi;

			internal readonly URI myUri;

			/// <exception cref="URISyntaxException"/>
			public InternalDirOfViewFs(InodeTree.INodeDir<AbstractFileSystem> dir, long cTime
				, UserGroupInformation ugi, URI uri)
				: base(FsConstants.ViewfsUri, FsConstants.ViewfsScheme, false, -1)
			{
				/*
				* An instance of this class represents an internal dir of the viewFs
				* ie internal dir of the mount table.
				* It is a ready only mount tbale and create, mkdir or delete operations
				* are not allowed.
				* If called on create or mkdir then this target is the parent of the
				* directory in which one is trying to create or mkdir; hence
				* in this case the path name passed in is the last component.
				* Otherwise this target is the end point of the path and hence
				* the path name passed in is null.
				*/
				// of the the mount table
				// the user/group of user who created mtable
				// the URI of the outer ViewFs
				theInternalDir = dir;
				creationTime = cTime;
				this.ugi = ugi;
				myUri = uri;
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

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
				, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
				, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
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

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
				)
			{
				CheckPathIsSlash(f);
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
					(myUri, null));
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			public override FileStatus GetFileLinkStatus(Path f)
			{
				// look up i internalDirs children - ignore first Slash
				InodeTree.INode<AbstractFileSystem> inode = theInternalDir.children[Runtime.Substring
					(f.ToUri().ToString(), 1)];
				if (inode == null)
				{
					throw new FileNotFoundException("viewFs internal mount table - missing entry:" + 
						f);
				}
				FileStatus result;
				if (inode is InodeTree.INodeLink)
				{
					InodeTree.INodeLink<AbstractFileSystem> inodelink = (InodeTree.INodeLink<AbstractFileSystem
						>)inode;
					result = new FileStatus(0, false, 0, 0, creationTime, creationTime, Constants.Permission555
						, ugi.GetUserName(), ugi.GetGroupNames()[0], inodelink.GetTargetLink(), new Path
						(inode.fullPath).MakeQualified(myUri, null));
				}
				else
				{
					result = new FileStatus(0, true, 0, 0, creationTime, creationTime, Constants.Permission555
						, ugi.GetUserName(), ugi.GetGroupNames()[0], new Path(inode.fullPath).MakeQualified
						(myUri, null));
				}
				return result;
			}

			public override FsStatus GetFsStatus()
			{
				return new FsStatus(0, 0, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FsServerDefaults GetServerDefaults()
			{
				throw new IOException("FsServerDefaults not implemented yet");
			}

			public override int GetUriDefaultPort()
			{
				return -1;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				CheckPathIsSlash(f);
				FileStatus[] result = new FileStatus[theInternalDir.children.Count];
				int i = 0;
				foreach (KeyValuePair<string, InodeTree.INode<AbstractFileSystem>> iEntry in theInternalDir
					.children)
				{
					InodeTree.INode<AbstractFileSystem> inode = iEntry.Value;
					if (inode is InodeTree.INodeLink)
					{
						InodeTree.INodeLink<AbstractFileSystem> link = (InodeTree.INodeLink<AbstractFileSystem
							>)inode;
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
			public override void Mkdir(Path dir, FsPermission permission, bool createParent)
			{
				if (theInternalDir.isRoot && dir == null)
				{
					throw new FileAlreadyExistsException("/ already exits");
				}
				throw ReadOnlyMountTable("mkdir", dir);
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Open(Path f, int bufferSize)
			{
				CheckPathIsSlash(f);
				throw new FileNotFoundException("Path points to dir not a file");
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool Truncate(Path f, long newLength)
			{
				CheckPathIsSlash(f);
				throw ReadOnlyMountTable("truncate", f);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void RenameInternal(Path src, Path dst)
			{
				CheckPathIsSlash(src);
				CheckPathIsSlash(dst);
				throw ReadOnlyMountTable("rename", src);
			}

			public override bool SupportsSymlinks()
			{
				return true;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			public override void CreateSymlink(Path target, Path link, bool createParent)
			{
				throw ReadOnlyMountTable("createSymlink", link);
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override Path GetLinkTarget(Path f)
			{
				return GetFileLinkStatus(f).GetSymlink();
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

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			public override void SetVerifyChecksum(bool verifyChecksum)
			{
				throw ReadOnlyMountTable("setVerifyChecksum", string.Empty);
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

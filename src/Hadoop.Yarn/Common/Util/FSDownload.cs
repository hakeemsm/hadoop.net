using System;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Cache;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>Download a single URL to the local disk.</summary>
	public class FSDownload : Callable<Path>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.FSDownload
			));

		private FileContext files;

		private readonly UserGroupInformation userUgi;

		private Configuration conf;

		private LocalResource resource;

		private readonly LoadingCache<Path, Future<FileStatus>> statCache;

		/// <summary>The local FS dir path under which this resource is to be localized to</summary>
		private Path destDirPath;

		private static readonly FsPermission cachePerms = new FsPermission((short)0x1ed);

		internal static readonly FsPermission PublicFilePerms = new FsPermission((short)0x16d
			);

		internal static readonly FsPermission PrivateFilePerms = new FsPermission((short)
			0x140);

		internal static readonly FsPermission PublicDirPerms = new FsPermission((short)0x1ed
			);

		internal static readonly FsPermission PrivateDirPerms = new FsPermission((short)0x1c0
			);

		public FSDownload(FileContext files, UserGroupInformation ugi, Configuration conf
			, Path destDirPath, LocalResource resource)
			: this(files, ugi, conf, destDirPath, resource, null)
		{
		}

		public FSDownload(FileContext files, UserGroupInformation ugi, Configuration conf
			, Path destDirPath, LocalResource resource, LoadingCache<Path, Future<FileStatus
			>> statCache)
		{
			this.conf = conf;
			this.destDirPath = destDirPath;
			this.files = files;
			this.userUgi = ugi;
			this.resource = resource;
			this.statCache = statCache;
		}

		internal virtual LocalResource GetResource()
		{
			return resource;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateDir(Path path, FsPermission perm)
		{
			files.Mkdir(path, perm, false);
			if (!perm.Equals(files.GetUMask().ApplyUMask(perm)))
			{
				files.SetPermission(path, perm);
			}
		}

		/// <summary>Creates the cache loader for the status loading cache.</summary>
		/// <remarks>
		/// Creates the cache loader for the status loading cache. This should be used
		/// to create an instance of the status cache that is passed into the
		/// FSDownload constructor.
		/// </remarks>
		public static CacheLoader<Path, Future<FileStatus>> CreateStatusCacheLoader(Configuration
			 conf)
		{
			return new _CacheLoader_119(conf);
		}

		private sealed class _CacheLoader_119 : CacheLoader<Path, Future<FileStatus>>
		{
			public _CacheLoader_119(Configuration conf)
			{
				this.conf = conf;
			}

			public override Future<FileStatus> Load(Path path)
			{
				try
				{
					FileSystem fs = path.GetFileSystem(conf);
					return Futures.ImmediateFuture(fs.GetFileStatus(path));
				}
				catch (Exception th)
				{
					// report failures so it can be memoized
					return Futures.ImmediateFailedFuture(th);
				}
			}

			private readonly Configuration conf;
		}

		/// <summary>
		/// Returns a boolean to denote whether a cache file is visible to all (public)
		/// or not
		/// </summary>
		/// <returns>
		/// true if the path in the current path is visible to all, false
		/// otherwise
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public static bool IsPublic(FileSystem fs, Path current, FileStatus sStat, LoadingCache
			<Path, Future<FileStatus>> statCache)
		{
			current = fs.MakeQualified(current);
			//the leaf level file should be readable by others
			if (!CheckPublicPermsForAll(fs, sStat, FsAction.ReadExecute, FsAction.Read))
			{
				return false;
			}
			if (Shell.Windows && fs is LocalFileSystem)
			{
				// Relax the requirement for public cache on LFS on Windows since default
				// permissions are "700" all the way up to the drive letter. In this
				// model, the only requirement for a user is to give EVERYONE group
				// permission on the file and the file will be considered public.
				// This code path is only hit when fs.default.name is file:/// (mainly
				// in tests).
				return true;
			}
			return AncestorsHaveExecutePermissions(fs, current.GetParent(), statCache);
		}

		/// <exception cref="System.IO.IOException"/>
		private static bool CheckPublicPermsForAll(FileSystem fs, FileStatus status, FsAction
			 dir, FsAction file)
		{
			FsPermission perms = status.GetPermission();
			FsAction otherAction = perms.GetOtherAction();
			if (status.IsDirectory())
			{
				if (!otherAction.Implies(dir))
				{
					return false;
				}
				foreach (FileStatus child in fs.ListStatus(status.GetPath()))
				{
					if (!CheckPublicPermsForAll(fs, child, dir, file))
					{
						return false;
					}
				}
				return true;
			}
			return (otherAction.Implies(file));
		}

		/// <summary>
		/// Returns true if all ancestors of the specified path have the 'execute'
		/// permission set for all users (i.e.
		/// </summary>
		/// <remarks>
		/// Returns true if all ancestors of the specified path have the 'execute'
		/// permission set for all users (i.e. that other users can traverse
		/// the directory hierarchy to the given path)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal static bool AncestorsHaveExecutePermissions(FileSystem fs, Path path, LoadingCache
			<Path, Future<FileStatus>> statCache)
		{
			Path current = path;
			while (current != null)
			{
				//the subdirs in the path should have execute permissions for others
				if (!CheckPermissionOfOther(fs, current, FsAction.Execute, statCache))
				{
					return false;
				}
				current = current.GetParent();
			}
			return true;
		}

		/// <summary>
		/// Checks for a given path whether the Other permissions on it
		/// imply the permission in the passed FsAction
		/// </summary>
		/// <param name="fs"/>
		/// <param name="path"/>
		/// <param name="action"/>
		/// <returns>true if the path in the uri is visible to all, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		private static bool CheckPermissionOfOther(FileSystem fs, Path path, FsAction action
			, LoadingCache<Path, Future<FileStatus>> statCache)
		{
			FileStatus status = GetFileStatus(fs, path, statCache);
			FsPermission perms = status.GetPermission();
			FsAction otherAction = perms.GetOtherAction();
			return otherAction.Implies(action);
		}

		/// <summary>
		/// Obtains the file status, first by checking the stat cache if it is
		/// available, and then by getting it explicitly from the filesystem.
		/// </summary>
		/// <remarks>
		/// Obtains the file status, first by checking the stat cache if it is
		/// available, and then by getting it explicitly from the filesystem. If we got
		/// the file status from the filesystem, it is added to the stat cache.
		/// The stat cache is expected to be managed by callers who provided it to
		/// FSDownload.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static FileStatus GetFileStatus(FileSystem fs, Path path, LoadingCache<Path
			, Future<FileStatus>> statCache)
		{
			// if the stat cache does not exist, simply query the filesystem
			if (statCache == null)
			{
				return fs.GetFileStatus(path);
			}
			try
			{
				// get or load it from the cache
				return statCache.Get(path).Get();
			}
			catch (ExecutionException e)
			{
				Exception cause = e.InnerException;
				// the underlying exception should normally be IOException
				if (cause is IOException)
				{
					throw (IOException)cause;
				}
				else
				{
					throw new IOException(cause);
				}
			}
			catch (Exception e)
			{
				// should not happen
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Path Copy(Path sCopy, Path dstdir)
		{
			FileSystem sourceFs = sCopy.GetFileSystem(conf);
			Path dCopy = new Path(dstdir, "tmp_" + sCopy.GetName());
			FileStatus sStat = sourceFs.GetFileStatus(sCopy);
			if (sStat.GetModificationTime() != resource.GetTimestamp())
			{
				throw new IOException("Resource " + sCopy + " changed on src filesystem (expected "
					 + resource.GetTimestamp() + ", was " + sStat.GetModificationTime());
			}
			if (resource.GetVisibility() == LocalResourceVisibility.Public)
			{
				if (!IsPublic(sourceFs, sCopy, sStat, statCache))
				{
					throw new IOException("Resource " + sCopy + " is not publicly accessable and as such cannot be part of the"
						 + " public cache.");
				}
			}
			FileUtil.Copy(sourceFs, sStat, FileSystem.GetLocal(conf), dCopy, false, true, conf
				);
			return dCopy;
		}

		/// <exception cref="System.IO.IOException"/>
		private long Unpack(FilePath localrsrc, FilePath dst)
		{
			switch (resource.GetType())
			{
				case LocalResourceType.Archive:
				{
					string lowerDst = StringUtils.ToLowerCase(dst.GetName());
					if (lowerDst.EndsWith(".jar"))
					{
						RunJar.UnJar(localrsrc, dst);
					}
					else
					{
						if (lowerDst.EndsWith(".zip"))
						{
							FileUtil.UnZip(localrsrc, dst);
						}
						else
						{
							if (lowerDst.EndsWith(".tar.gz") || lowerDst.EndsWith(".tgz") || lowerDst.EndsWith
								(".tar"))
							{
								FileUtil.UnTar(localrsrc, dst);
							}
							else
							{
								Log.Warn("Cannot unpack " + localrsrc);
								if (!localrsrc.RenameTo(dst))
								{
									throw new IOException("Unable to rename file: [" + localrsrc + "] to [" + dst + "]"
										);
								}
							}
						}
					}
					break;
				}

				case LocalResourceType.Pattern:
				{
					string lowerDst = StringUtils.ToLowerCase(dst.GetName());
					if (lowerDst.EndsWith(".jar"))
					{
						string p = resource.GetPattern();
						RunJar.UnJar(localrsrc, dst, p == null ? RunJar.MatchAny : Sharpen.Pattern.Compile
							(p));
						FilePath newDst = new FilePath(dst, dst.GetName());
						if (!dst.Exists() && !dst.Mkdir())
						{
							throw new IOException("Unable to create directory: [" + dst + "]");
						}
						if (!localrsrc.RenameTo(newDst))
						{
							throw new IOException("Unable to rename file: [" + localrsrc + "] to [" + newDst 
								+ "]");
						}
					}
					else
					{
						if (lowerDst.EndsWith(".zip"))
						{
							Log.Warn("Treating [" + localrsrc + "] as an archive even though it " + "was specified as PATTERN"
								);
							FileUtil.UnZip(localrsrc, dst);
						}
						else
						{
							if (lowerDst.EndsWith(".tar.gz") || lowerDst.EndsWith(".tgz") || lowerDst.EndsWith
								(".tar"))
							{
								Log.Warn("Treating [" + localrsrc + "] as an archive even though it " + "was specified as PATTERN"
									);
								FileUtil.UnTar(localrsrc, dst);
							}
							else
							{
								Log.Warn("Cannot unpack " + localrsrc);
								if (!localrsrc.RenameTo(dst))
								{
									throw new IOException("Unable to rename file: [" + localrsrc + "] to [" + dst + "]"
										);
								}
							}
						}
					}
					break;
				}

				case LocalResourceType.File:
				default:
				{
					if (!localrsrc.RenameTo(dst))
					{
						throw new IOException("Unable to rename file: [" + localrsrc + "] to [" + dst + "]"
							);
					}
					break;
				}
			}
			if (localrsrc.IsFile())
			{
				try
				{
					files.Delete(new Path(localrsrc.ToString()), false);
				}
				catch (IOException)
				{
				}
			}
			return 0;
		}

		// TODO Should calculate here before returning
		//return FileUtil.getDU(destDir);
		/// <exception cref="System.Exception"/>
		public virtual Path Call()
		{
			Path sCopy;
			try
			{
				sCopy = ConverterUtils.GetPathFromYarnURL(resource.GetResource());
			}
			catch (URISyntaxException e)
			{
				throw new IOException("Invalid resource", e);
			}
			CreateDir(destDirPath, cachePerms);
			Path dst_work = new Path(destDirPath + "_tmp");
			CreateDir(dst_work, cachePerms);
			Path dFinal = files.MakeQualified(new Path(dst_work, sCopy.GetName()));
			try
			{
				Path dTmp = null == userUgi ? files.MakeQualified(Copy(sCopy, dst_work)) : userUgi
					.DoAs(new _PrivilegedExceptionAction_359(this, sCopy, dst_work));
				Unpack(new FilePath(dTmp.ToUri()), new FilePath(dFinal.ToUri()));
				ChangePermissions(dFinal.GetFileSystem(conf), dFinal);
				files.Rename(dst_work, destDirPath, Options.Rename.Overwrite);
			}
			catch (Exception e)
			{
				try
				{
					files.Delete(destDirPath, true);
				}
				catch (IOException)
				{
				}
				throw;
			}
			finally
			{
				try
				{
					files.Delete(dst_work, true);
				}
				catch (FileNotFoundException)
				{
				}
				conf = null;
				resource = null;
			}
			return files.MakeQualified(new Path(destDirPath, sCopy.GetName()));
		}

		private sealed class _PrivilegedExceptionAction_359 : PrivilegedExceptionAction<Path
			>
		{
			public _PrivilegedExceptionAction_359(FSDownload _enclosing, Path sCopy, Path dst_work
				)
			{
				this._enclosing = _enclosing;
				this.sCopy = sCopy;
				this.dst_work = dst_work;
			}

			/// <exception cref="System.Exception"/>
			public Path Run()
			{
				return this._enclosing.files.MakeQualified(this._enclosing.Copy(sCopy, dst_work));
			}

			private readonly FSDownload _enclosing;

			private readonly Path sCopy;

			private readonly Path dst_work;
		}

		/// <summary>
		/// Recursively change permissions of all files/dirs on path based
		/// on resource visibility.
		/// </summary>
		/// <remarks>
		/// Recursively change permissions of all files/dirs on path based
		/// on resource visibility.
		/// Change to 755 or 700 for dirs, 555 or 500 for files.
		/// </remarks>
		/// <param name="fs">FileSystem</param>
		/// <param name="path">Path to modify perms for</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		private void ChangePermissions(FileSystem fs, Path path)
		{
			FilePath f = new FilePath(path.ToUri());
			if (FileUtils.IsSymlink(f))
			{
				// avoid following symlinks when changing permissions
				return;
			}
			bool isDir = f.IsDirectory();
			FsPermission perm = cachePerms;
			// set public perms as 755 or 555 based on dir or file
			if (resource.GetVisibility() == LocalResourceVisibility.Public)
			{
				perm = isDir ? PublicDirPerms : PublicFilePerms;
			}
			else
			{
				// set private perms as 700 or 500
				// PRIVATE:
				// APPLICATION:
				perm = isDir ? PrivateDirPerms : PrivateFilePerms;
			}
			Log.Debug("Changing permissions for path " + path + " to perm " + perm);
			FsPermission fPerm = perm;
			if (null == userUgi)
			{
				files.SetPermission(path, perm);
			}
			else
			{
				userUgi.DoAs(new _PrivilegedExceptionAction_419(this, path, fPerm));
			}
			if (isDir)
			{
				FileStatus[] statuses = fs.ListStatus(path);
				foreach (FileStatus status in statuses)
				{
					ChangePermissions(fs, status.GetPath());
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_419 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_419(FSDownload _enclosing, Path path, FsPermission
				 fPerm)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.fPerm = fPerm;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.files.SetPermission(path, fPerm);
				return null;
			}

			private readonly FSDownload _enclosing;

			private readonly Path path;

			private readonly FsPermission fPerm;
		}
	}
}

using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	/// <summary>
	/// This class is a container of multiple thread pools, each for a volume,
	/// so that we can schedule async disk operations easily.
	/// </summary>
	/// <remarks>
	/// This class is a container of multiple thread pools, each for a volume,
	/// so that we can schedule async disk operations easily.
	/// Examples of async disk operations are deletion of files.
	/// We can move the files to a "toBeDeleted" folder before asychronously
	/// deleting it, to make sure the caller can run it faster.
	/// Users should not write files into the "toBeDeleted" folder, otherwise
	/// the files can be gone any time we restart the MRAsyncDiskService.
	/// This class also contains all operations that will be performed by the
	/// thread pools.
	/// </remarks>
	public class MRAsyncDiskService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Util.MRAsyncDiskService
			));

		internal AsyncDiskService asyncDiskService;

		public const string Tobedeleted = "toBeDeleted";

		/// <summary>
		/// Create a AsyncDiskServices with a set of volumes (specified by their
		/// root directories).
		/// </summary>
		/// <remarks>
		/// Create a AsyncDiskServices with a set of volumes (specified by their
		/// root directories).
		/// The AsyncDiskServices uses one ThreadPool per volume to do the async disk
		/// operations.
		/// </remarks>
		/// <param name="localFileSystem">The localFileSystem used for deletions.</param>
		/// <param name="nonCanonicalVols">
		/// The roots of the file system volumes, which may
		/// be absolte paths, or paths relative to the ${user.dir} system property
		/// ("cwd").
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public MRAsyncDiskService(FileSystem localFileSystem, params string[] nonCanonicalVols
			)
		{
			this.localFileSystem = localFileSystem;
			this.volumes = new string[nonCanonicalVols.Length];
			for (int v = 0; v < nonCanonicalVols.Length; v++)
			{
				this.volumes[v] = NormalizePath(nonCanonicalVols[v]);
				Log.Debug("Normalized volume: " + nonCanonicalVols[v] + " -> " + this.volumes[v]);
			}
			asyncDiskService = new AsyncDiskService(this.volumes);
			// Create one ThreadPool per volume
			for (int v_1 = 0; v_1 < volumes.Length; v_1++)
			{
				// Create the root for file deletion
				Path absoluteSubdir = new Path(volumes[v_1], Tobedeleted);
				if (!localFileSystem.Mkdirs(absoluteSubdir))
				{
					// We should tolerate missing volumes. 
					Log.Warn("Cannot create " + Tobedeleted + " in " + volumes[v_1] + ". Ignored.");
				}
			}
			// Create tasks to delete the paths inside the volumes
			for (int v_2 = 0; v_2 < volumes.Length; v_2++)
			{
				Path absoluteSubdir = new Path(volumes[v_2], Tobedeleted);
				FileStatus[] files = null;
				try
				{
					// List all files inside the volumes TOBEDELETED sub directory
					files = localFileSystem.ListStatus(absoluteSubdir);
				}
				catch (Exception)
				{
				}
				// Ignore exceptions in listStatus
				// We tolerate missing sub directories.
				if (files != null)
				{
					for (int f = 0; f < files.Length; f++)
					{
						// Get the relative file name to the root of the volume
						string absoluteFilename = files[f].GetPath().ToUri().GetPath();
						string relative = Tobedeleted + Path.SeparatorChar + files[f].GetPath().GetName();
						MRAsyncDiskService.DeleteTask task = new MRAsyncDiskService.DeleteTask(this, volumes
							[v_2], absoluteFilename, relative);
						Execute(volumes[v_2], task);
					}
				}
			}
		}

		/// <summary>Initialize MRAsyncDiskService based on conf.</summary>
		/// <param name="conf">local file system and local dirs will be read from conf</param>
		/// <exception cref="System.IO.IOException"/>
		public MRAsyncDiskService(JobConf conf)
			: this(FileSystem.GetLocal(conf), conf.GetLocalDirs())
		{
		}

		/// <summary>Execute the task sometime in the future, using ThreadPools.</summary>
		internal virtual void Execute(string root, Runnable task)
		{
			lock (this)
			{
				asyncDiskService.Execute(root, task);
			}
		}

		/// <summary>Gracefully start the shut down of all ThreadPools.</summary>
		public virtual void Shutdown()
		{
			lock (this)
			{
				asyncDiskService.Shutdown();
			}
		}

		/// <summary>Shut down all ThreadPools immediately.</summary>
		public virtual IList<Runnable> ShutdownNow()
		{
			lock (this)
			{
				return asyncDiskService.ShutdownNow();
			}
		}

		/// <summary>Wait for the termination of the thread pools.</summary>
		/// <param name="milliseconds">The number of milliseconds to wait</param>
		/// <returns>true if all thread pools are terminated within time limit</returns>
		/// <exception cref="System.Exception"></exception>
		public virtual bool AwaitTermination(long milliseconds)
		{
			lock (this)
			{
				return asyncDiskService.AwaitTermination(milliseconds);
			}
		}

		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");

		private FileSystem localFileSystem;

		private string[] volumes;

		private static AtomicLong uniqueId = new AtomicLong(0);

		/// <summary>A task for deleting a pathName from a volume.</summary>
		internal class DeleteTask : Runnable
		{
			/// <summary>The volume that the file is on</summary>
			internal string volume;

			/// <summary>The file name before the move</summary>
			internal string originalPath;

			/// <summary>The file name after the move</summary>
			internal string pathToBeDeleted;

			/// <summary>Delete a file/directory (recursively if needed).</summary>
			/// <param name="volume">The volume that the file/dir is in.</param>
			/// <param name="originalPath">The original name, relative to volume root.</param>
			/// <param name="pathToBeDeleted">
			/// The name after the move, relative to volume root,
			/// containing TOBEDELETED.
			/// </param>
			internal DeleteTask(MRAsyncDiskService _enclosing, string volume, string originalPath
				, string pathToBeDeleted)
			{
				this._enclosing = _enclosing;
				this.volume = volume;
				this.originalPath = originalPath;
				this.pathToBeDeleted = pathToBeDeleted;
			}

			public override string ToString()
			{
				// Called in AsyncDiskService.execute for displaying error messages.
				return "deletion of " + this.pathToBeDeleted + " on " + this.volume + " with original name "
					 + this.originalPath;
			}

			public virtual void Run()
			{
				bool success = false;
				Exception e = null;
				try
				{
					Path absolutePathToBeDeleted = new Path(this.volume, this.pathToBeDeleted);
					success = this._enclosing.localFileSystem.Delete(absolutePathToBeDeleted, true);
				}
				catch (Exception ex)
				{
					e = ex;
				}
				if (!success)
				{
					if (e != null)
					{
						MRAsyncDiskService.Log.Warn("Failure in " + this + " with exception " + StringUtils
							.StringifyException(e));
					}
					else
					{
						MRAsyncDiskService.Log.Warn("Failure in " + this);
					}
				}
				else
				{
					MRAsyncDiskService.Log.Debug("Successfully did " + this.ToString());
				}
			}

			private readonly MRAsyncDiskService _enclosing;
		}

		/// <summary>
		/// Move the path name on one volume to a temporary location and then
		/// delete them.
		/// </summary>
		/// <remarks>
		/// Move the path name on one volume to a temporary location and then
		/// delete them.
		/// This functions returns when the moves are done, but not necessarily all
		/// deletions are done. This is usually good enough because applications
		/// won't see the path name under the old name anyway after the move.
		/// </remarks>
		/// <param name="volume">The disk volume</param>
		/// <param name="pathName">The path name relative to volume root.</param>
		/// <exception cref="System.IO.IOException">If the move failed</exception>
		/// <returns>false     if the file is not found</returns>
		public virtual bool MoveAndDeleteRelativePath(string volume, string pathName)
		{
			volume = NormalizePath(volume);
			// Move the file right now, so that it can be deleted later
			string newPathName = format.Format(new DateTime()) + "_" + uniqueId.GetAndIncrement
				();
			newPathName = Tobedeleted + Path.SeparatorChar + newPathName;
			Path source = new Path(volume, pathName);
			Path target = new Path(volume, newPathName);
			try
			{
				if (!localFileSystem.Rename(source, target))
				{
					// If the source does not exists, return false.
					// This is necessary because rename can return false if the source  
					// does not exists.
					if (!localFileSystem.Exists(source))
					{
						return false;
					}
					// Try to recreate the parent directory just in case it gets deleted.
					if (!localFileSystem.Mkdirs(new Path(volume, Tobedeleted)))
					{
						throw new IOException("Cannot create " + Tobedeleted + " under " + volume);
					}
					// Try rename again. If it fails, return false.
					if (!localFileSystem.Rename(source, target))
					{
						throw new IOException("Cannot rename " + source + " to " + target);
					}
				}
			}
			catch (FileNotFoundException)
			{
				// Return false in case that the file is not found.  
				return false;
			}
			MRAsyncDiskService.DeleteTask task = new MRAsyncDiskService.DeleteTask(this, volume
				, pathName, newPathName);
			Execute(volume, task);
			return true;
		}

		/// <summary>
		/// Move the path name on each volume to a temporary location and then
		/// delete them.
		/// </summary>
		/// <remarks>
		/// Move the path name on each volume to a temporary location and then
		/// delete them.
		/// This functions returns when the moves are done, but not necessarily all
		/// deletions are done. This is usually good enough because applications
		/// won't see the path name under the old name anyway after the move.
		/// </remarks>
		/// <param name="pathName">The path name relative to each volume root</param>
		/// <exception cref="System.IO.IOException">If any of the move failed</exception>
		/// <returns>
		/// false     If any of the target pathName did not exist,
		/// note that the operation is still done on all volumes.
		/// </returns>
		public virtual bool MoveAndDeleteFromEachVolume(string pathName)
		{
			bool result = true;
			for (int i = 0; i < volumes.Length; i++)
			{
				result = result && MoveAndDeleteRelativePath(volumes[i], pathName);
			}
			return result;
		}

		/// <summary>
		/// Move all files/directories inside volume into TOBEDELETED, and then
		/// delete them.
		/// </summary>
		/// <remarks>
		/// Move all files/directories inside volume into TOBEDELETED, and then
		/// delete them.  The TOBEDELETED directory itself is ignored.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CleanupAllVolumes()
		{
			for (int v = 0; v < volumes.Length; v++)
			{
				// List all files inside the volumes
				FileStatus[] files = null;
				try
				{
					files = localFileSystem.ListStatus(new Path(volumes[v]));
				}
				catch (Exception)
				{
				}
				// Ignore exceptions in listStatus
				// We tolerate missing volumes.
				if (files != null)
				{
					for (int f = 0; f < files.Length; f++)
					{
						// Get the file name - the last component of the Path
						string entryName = files[f].GetPath().GetName();
						// Do not delete the current TOBEDELETED
						if (!Tobedeleted.Equals(entryName))
						{
							MoveAndDeleteRelativePath(volumes[v], entryName);
						}
					}
				}
			}
		}

		/// <summary>Returns the normalized path of a path.</summary>
		private string NormalizePath(string path)
		{
			return (new Path(path)).MakeQualified(this.localFileSystem).ToUri().GetPath();
		}

		/// <summary>Get the relative path name with respect to the root of the volume.</summary>
		/// <param name="absolutePathName">The absolute path name</param>
		/// <param name="volume">Root of the volume.</param>
		/// <returns>null if the absolute path name is outside of the volume.</returns>
		private string GetRelativePathName(string absolutePathName, string volume)
		{
			absolutePathName = NormalizePath(absolutePathName);
			// Get the file names
			if (!absolutePathName.StartsWith(volume))
			{
				return null;
			}
			// Get rid of the volume prefix
			string fileName = Sharpen.Runtime.Substring(absolutePathName, volume.Length);
			if (fileName[0] == Path.SeparatorChar)
			{
				fileName = Sharpen.Runtime.Substring(fileName, 1);
			}
			return fileName;
		}

		/// <summary>Move the path name to a temporary location and then delete it.</summary>
		/// <remarks>
		/// Move the path name to a temporary location and then delete it.
		/// Note that if there is no volume that contains this path, the path
		/// will stay as it is, and the function will return false.
		/// This functions returns when the moves are done, but not necessarily all
		/// deletions are done. This is usually good enough because applications
		/// won't see the path name under the old name anyway after the move.
		/// </remarks>
		/// <param name="absolutePathName">The path name from root "/"</param>
		/// <exception cref="System.IO.IOException">If the move failed</exception>
		/// <returns>false if we are unable to move the path name</returns>
		public virtual bool MoveAndDeleteAbsolutePath(string absolutePathName)
		{
			for (int v = 0; v < volumes.Length; v++)
			{
				string relative = GetRelativePathName(absolutePathName, volumes[v]);
				if (relative != null)
				{
					return MoveAndDeleteRelativePath(volumes[v], relative);
				}
			}
			throw new IOException("Cannot delete " + absolutePathName + " because it's outside of all volumes."
				);
		}
	}
}

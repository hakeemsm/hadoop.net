using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Sharedcache;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// The task that runs and cleans up the shared cache area for stale entries and
	/// orphaned files.
	/// </summary>
	/// <remarks>
	/// The task that runs and cleans up the shared cache area for stale entries and
	/// orphaned files. It is expected that only one cleaner task runs at any given
	/// point in time.
	/// </remarks>
	internal class CleanerTask : Runnable
	{
		private const string RenamedSuffix = "-renamed";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.CleanerTask
			));

		private readonly string location;

		private readonly long sleepTime;

		private readonly int nestedLevel;

		private readonly Path root;

		private readonly FileSystem fs;

		private readonly SCMStore store;

		private readonly CleanerMetrics metrics;

		private readonly Lock cleanerTaskLock;

		/// <summary>Creates a cleaner task based on the configuration.</summary>
		/// <remarks>
		/// Creates a cleaner task based on the configuration. This is provided for
		/// convenience.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="store"/>
		/// <param name="metrics"/>
		/// <param name="cleanerTaskLock">
		/// lock that ensures a serial execution of cleaner
		/// task
		/// </param>
		/// <returns>an instance of a CleanerTask</returns>
		public static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.CleanerTask Create
			(Configuration conf, SCMStore store, CleanerMetrics metrics, Lock cleanerTaskLock
			)
		{
			try
			{
				// get the root directory for the shared cache
				string location = conf.Get(YarnConfiguration.SharedCacheRoot, YarnConfiguration.DefaultSharedCacheRoot
					);
				long sleepTime = conf.GetLong(YarnConfiguration.ScmCleanerResourceSleepMs, YarnConfiguration
					.DefaultScmCleanerResourceSleepMs);
				int nestedLevel = SharedCacheUtil.GetCacheDepth(conf);
				FileSystem fs = FileSystem.Get(conf);
				return new Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.CleanerTask(location, 
					sleepTime, nestedLevel, fs, store, metrics, cleanerTaskLock);
			}
			catch (IOException e)
			{
				Log.Error("Unable to obtain the filesystem for the cleaner service", e);
				throw new ExceptionInInitializerError(e);
			}
		}

		/// <summary>
		/// Creates a cleaner task based on the root directory location and the
		/// filesystem.
		/// </summary>
		internal CleanerTask(string location, long sleepTime, int nestedLevel, FileSystem
			 fs, SCMStore store, CleanerMetrics metrics, Lock cleanerTaskLock)
		{
			this.location = location;
			this.sleepTime = sleepTime;
			this.nestedLevel = nestedLevel;
			this.root = new Path(location);
			this.fs = fs;
			this.store = store;
			this.metrics = metrics;
			this.cleanerTaskLock = cleanerTaskLock;
		}

		public virtual void Run()
		{
			if (!this.cleanerTaskLock.TryLock())
			{
				// there is already another task running
				Log.Warn("A cleaner task is already running. " + "This scheduled cleaner task will do nothing."
					);
				return;
			}
			try
			{
				if (!fs.Exists(root))
				{
					Log.Error("The shared cache root " + location + " was not found. " + "The cleaner task will do nothing."
						);
					return;
				}
				// we're now ready to process the shared cache area
				Process();
			}
			catch (Exception e)
			{
				Log.Error("Unexpected exception while initializing the cleaner task. " + "This task will do nothing,"
					, e);
			}
			finally
			{
				// this is set to false regardless of if it is a scheduled or on-demand
				// task
				this.cleanerTaskLock.Unlock();
			}
		}

		/// <summary>
		/// Sweeps and processes the shared cache area to clean up stale and orphaned
		/// files.
		/// </summary>
		internal virtual void Process()
		{
			// mark the beginning of the run in the metrics
			metrics.ReportCleaningStart();
			try
			{
				// now traverse individual directories and process them
				// the directory structure is specified by the nested level parameter
				// (e.g. 9/c/d/<checksum>)
				string pattern = SharedCacheUtil.GetCacheEntryGlobPattern(nestedLevel);
				FileStatus[] resources = fs.GlobStatus(new Path(root, pattern));
				int numResources = resources == null ? 0 : resources.Length;
				Log.Info("Processing " + numResources + " resources in the shared cache");
				long beginMs = Runtime.CurrentTimeMillis();
				if (resources != null)
				{
					foreach (FileStatus resource in resources)
					{
						// check for interruption so it can abort in a timely manner in case
						// of shutdown
						if (Sharpen.Thread.CurrentThread().IsInterrupted())
						{
							Log.Warn("The cleaner task was interrupted. Aborting.");
							break;
						}
						if (resource.IsDirectory())
						{
							ProcessSingleResource(resource);
						}
						else
						{
							Log.Warn("Invalid file at path " + resource.GetPath().ToString() + " when a directory was expected"
								);
						}
						// add sleep time between cleaning each directory if it is non-zero
						if (sleepTime > 0)
						{
							Sharpen.Thread.Sleep(sleepTime);
						}
					}
				}
				long endMs = Runtime.CurrentTimeMillis();
				long durationMs = endMs - beginMs;
				Log.Info("Processed " + numResources + " resource(s) in " + durationMs + " ms.");
			}
			catch (IOException e1)
			{
				Log.Error("Unable to complete the cleaner task", e1);
			}
			catch (Exception)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
			}
		}

		// restore the interrupt
		/// <summary>Returns a path for the root directory for the shared cache.</summary>
		internal virtual Path GetRootPath()
		{
			return root;
		}

		/// <summary>Processes a single shared cache resource directory.</summary>
		internal virtual void ProcessSingleResource(FileStatus resource)
		{
			Path path = resource.GetPath();
			// indicates the processing status of the resource
			CleanerTask.ResourceStatus resourceStatus = CleanerTask.ResourceStatus.Init;
			// first, if the path ends with the renamed suffix, it indicates the
			// directory was moved (as stale) but somehow not deleted (probably due to
			// SCM failure); delete the directory
			if (path.ToString().EndsWith(RenamedSuffix))
			{
				Log.Info("Found a renamed directory that was left undeleted at " + path.ToString(
					) + ". Deleting.");
				try
				{
					if (fs.Delete(path, true))
					{
						resourceStatus = CleanerTask.ResourceStatus.Deleted;
					}
				}
				catch (IOException e)
				{
					Log.Error("Error while processing a shared cache resource: " + path, e);
				}
			}
			else
			{
				// this is the path to the cache resource directory
				// the directory name is the resource key (i.e. a unique identifier)
				string key = path.GetName();
				try
				{
					store.CleanResourceReferences(key);
				}
				catch (YarnException e)
				{
					Log.Error("Exception thrown while removing dead appIds.", e);
				}
				if (store.IsResourceEvictable(key, resource))
				{
					try
					{
						/*
						* TODO See YARN-2663: There is a race condition between
						* store.removeResource(key) and
						* removeResourceFromCacheFileSystem(path) operations because they do
						* not happen atomically and resources can be uploaded with different
						* file names by the node managers.
						*/
						// remove the resource from scm (checks for appIds as well)
						if (store.RemoveResource(key))
						{
							// remove the resource from the file system
							bool deleted = RemoveResourceFromCacheFileSystem(path);
							if (deleted)
							{
								resourceStatus = CleanerTask.ResourceStatus.Deleted;
							}
							else
							{
								Log.Error("Failed to remove path from the file system." + " Skipping this resource: "
									 + path);
								resourceStatus = CleanerTask.ResourceStatus.Error;
							}
						}
						else
						{
							// we did not delete the resource because it contained application
							// ids
							resourceStatus = CleanerTask.ResourceStatus.Processed;
						}
					}
					catch (IOException e)
					{
						Log.Error("Failed to remove path from the file system. Skipping this resource: " 
							+ path, e);
						resourceStatus = CleanerTask.ResourceStatus.Error;
					}
				}
				else
				{
					resourceStatus = CleanerTask.ResourceStatus.Processed;
				}
			}
			switch (resourceStatus)
			{
				case CleanerTask.ResourceStatus.Deleted:
				{
					// record the processing
					metrics.ReportAFileDelete();
					break;
				}

				case CleanerTask.ResourceStatus.Processed:
				{
					metrics.ReportAFileProcess();
					break;
				}

				case CleanerTask.ResourceStatus.Error:
				{
					metrics.ReportAFileError();
					break;
				}

				default:
				{
					Log.Error("Cleaner encountered an invalid status (" + resourceStatus + ") while processing resource: "
						 + path.GetName());
					break;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool RemoveResourceFromCacheFileSystem(Path path)
		{
			// rename the directory to make the delete atomic
			Path renamedPath = new Path(path.ToString() + RenamedSuffix);
			if (fs.Rename(path, renamedPath))
			{
				// the directory can be removed safely now
				// log the original path
				Log.Info("Deleting " + path.ToString());
				return fs.Delete(renamedPath, true);
			}
			else
			{
				// we were unable to remove it for some reason: it's best to leave
				// it at that
				Log.Error("We were not able to rename the directory to " + renamedPath.ToString()
					 + ". We will leave it intact.");
			}
			return false;
		}

		/// <summary>
		/// A status indicating what happened with the processing of a given cache
		/// resource.
		/// </summary>
		private enum ResourceStatus
		{
			Init,
			Processed,
			Deleted,
			Error
		}
	}
}

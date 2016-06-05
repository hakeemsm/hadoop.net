using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Sharedcache;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store
{
	/// <summary>A thread safe version of an in-memory SCM store.</summary>
	/// <remarks>
	/// A thread safe version of an in-memory SCM store. The thread safety is
	/// implemented with two key pieces: (1) at the mapping level a ConcurrentHashMap
	/// is used to allow concurrency to resources and their associated references,
	/// and (2) a key level lock is used to ensure mutual exclusion between any
	/// operation that accesses a resource with the same key. <br />
	/// <br />
	/// To ensure safe key-level locking, we use the original string key and intern
	/// it weakly using hadoop's <code>StringInterner</code>. It avoids the pitfalls
	/// of using built-in String interning. The interned strings are also weakly
	/// referenced, so it can be garbage collected once it is done. And there is
	/// little risk of keys being available for other parts of the code so they can
	/// be used as locks accidentally. <br />
	/// <br />
	/// Resources in the in-memory store are evicted based on a time staleness
	/// criteria. If a resource is not referenced (i.e. used) for a given period, it
	/// is designated as a stale resource and is considered evictable.
	/// </remarks>
	public class InMemorySCMStore : SCMStore
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store.InMemorySCMStore
			));

		private readonly IDictionary<string, SharedCacheResource> cachedResources = new ConcurrentHashMap
			<string, SharedCacheResource>();

		private ICollection<ApplicationId> initialApps = new AList<ApplicationId>();

		private readonly object initialAppsLock = new object();

		private long startTime;

		private int stalenessMinutes;

		private ScheduledExecutorService scheduler;

		private int initialDelayMin;

		private int checkPeriodMin;

		public InMemorySCMStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store.InMemorySCMStore
				).FullName)
		{
		}

		[VisibleForTesting]
		public InMemorySCMStore(AppChecker appChecker)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store.InMemorySCMStore
				).FullName, appChecker)
		{
		}

		private string Intern(string key)
		{
			return StringInterner.WeakIntern(key);
		}

		/// <summary>
		/// The in-memory store bootstraps itself from the shared cache entries that
		/// exist in HDFS.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.startTime = Runtime.CurrentTimeMillis();
			this.initialDelayMin = GetInitialDelay(conf);
			this.checkPeriodMin = GetCheckPeriod(conf);
			this.stalenessMinutes = GetStalenessPeriod(conf);
			Bootstrap(conf);
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("InMemorySCMStore").Build
				();
			scheduler = Executors.NewSingleThreadScheduledExecutor(tf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// start composed services first
			base.ServiceStart();
			// Get initial list of running applications
			Log.Info("Getting the active app list to initialize the in-memory scm store");
			lock (initialAppsLock)
			{
				initialApps = appChecker.GetActiveApplications();
			}
			Log.Info(initialApps.Count + " apps recorded as active at this time");
			Runnable task = new InMemorySCMStore.AppCheckTask(this, appChecker);
			scheduler.ScheduleAtFixedRate(task, initialDelayMin, checkPeriodMin, TimeUnit.Minutes
				);
			Log.Info("Scheduled the in-memory scm store app check task to run every " + checkPeriodMin
				 + " minutes.");
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			Log.Info("Stopping the " + typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store.InMemorySCMStore
				).Name + " service.");
			if (scheduler != null)
			{
				Log.Info("Shutting down the background thread.");
				scheduler.ShutdownNow();
				try
				{
					if (!scheduler.AwaitTermination(10, TimeUnit.Seconds))
					{
						Log.Warn("Gave up waiting for the app check task to shutdown.");
					}
				}
				catch (Exception e)
				{
					Log.Warn("The InMemorySCMStore was interrupted while shutting down the " + "app check task."
						, e);
				}
				Log.Info("The background thread stopped.");
			}
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Bootstrap(Configuration conf)
		{
			IDictionary<string, string> initialCachedResources = GetInitialCachedResources(FileSystem
				.Get(conf), conf);
			Log.Info("Bootstrapping from " + initialCachedResources.Count + " cache resources located in the file system"
				);
			IEnumerator<KeyValuePair<string, string>> it = initialCachedResources.GetEnumerator
				();
			while (it.HasNext())
			{
				KeyValuePair<string, string> e = it.Next();
				string key = Intern(e.Key);
				string fileName = e.Value;
				SharedCacheResource resource = new SharedCacheResource(fileName);
				// we don't hold the lock for this as it is done as part of serviceInit
				cachedResources[key] = resource;
				// clear out the initial resource to reduce the footprint
				it.Remove();
			}
			Log.Info("Bootstrapping complete");
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual IDictionary<string, string> GetInitialCachedResources(FileSystem
			 fs, Configuration conf)
		{
			// get the root directory for the shared cache
			string location = conf.Get(YarnConfiguration.SharedCacheRoot, YarnConfiguration.DefaultSharedCacheRoot
				);
			Path root = new Path(location);
			if (!fs.Exists(root))
			{
				string message = "The shared cache root directory " + location + " was not found";
				Log.Error(message);
				throw new IOException(message);
			}
			int nestedLevel = SharedCacheUtil.GetCacheDepth(conf);
			// now traverse individual directories and process them
			// the directory structure is specified by the nested level parameter
			// (e.g. 9/c/d/<checksum>/file)
			string pattern = SharedCacheUtil.GetCacheEntryGlobPattern(nestedLevel + 1);
			Log.Info("Querying for all individual cached resource files");
			FileStatus[] entries = fs.GlobStatus(new Path(root, pattern));
			int numEntries = entries == null ? 0 : entries.Length;
			Log.Info("Found " + numEntries + " files: processing for one resource per " + "key"
				);
			IDictionary<string, string> initialCachedEntries = new Dictionary<string, string>
				();
			if (entries != null)
			{
				foreach (FileStatus entry in entries)
				{
					Path file = entry.GetPath();
					string fileName = file.GetName();
					if (entry.IsFile())
					{
						// get the parent to get the checksum
						Path parent = file.GetParent();
						if (parent != null)
						{
							// the name of the immediate parent directory is the checksum
							string key = parent.GetName();
							// make sure we insert only one file per checksum whichever comes
							// first
							if (initialCachedEntries.Contains(key))
							{
								Log.Warn("Key " + key + " is already mapped to file " + initialCachedEntries[key]
									 + "; file " + fileName + " will not be added");
							}
							else
							{
								initialCachedEntries[key] = fileName;
							}
						}
					}
				}
			}
			Log.Info("A total of " + initialCachedEntries.Count + " files are now mapped");
			return initialCachedEntries;
		}

		/// <summary>Adds the given resource to the store under the key and the filename.</summary>
		/// <remarks>
		/// Adds the given resource to the store under the key and the filename. If the
		/// entry is already found, it returns the existing filename. It represents the
		/// state of the store at the time of this query. The entry may change or even
		/// be removed once this method returns. The caller should be prepared to
		/// handle that situation.
		/// </remarks>
		/// <returns>
		/// the filename of the newly inserted resource or that of the existing
		/// resource
		/// </returns>
		public override string AddResource(string key, string fileName)
		{
			string interned = Intern(key);
			lock (interned)
			{
				SharedCacheResource resource = cachedResources[interned];
				if (resource == null)
				{
					resource = new SharedCacheResource(fileName);
					cachedResources[interned] = resource;
				}
				return resource.GetFileName();
			}
		}

		/// <summary>
		/// Adds the provided resource reference to the cache resource under the key,
		/// and updates the access time.
		/// </summary>
		/// <remarks>
		/// Adds the provided resource reference to the cache resource under the key,
		/// and updates the access time. If it returns a non-null value, the caller may
		/// safely assume that the resource will not be removed at least until the app
		/// in this resource reference has terminated.
		/// </remarks>
		/// <returns>the filename of the resource, or null if the resource is not found</returns>
		public override string AddResourceReference(string key, SharedCacheResourceReference
			 @ref)
		{
			string interned = Intern(key);
			lock (interned)
			{
				SharedCacheResource resource = cachedResources[interned];
				if (resource == null)
				{
					// it's not mapped
					return null;
				}
				resource.AddReference(@ref);
				resource.UpdateAccessTime();
				return resource.GetFileName();
			}
		}

		/// <summary>
		/// Returns the list of resource references currently registered under the
		/// cache entry.
		/// </summary>
		/// <remarks>
		/// Returns the list of resource references currently registered under the
		/// cache entry. If the list is empty, it returns an empty collection. The
		/// returned collection is unmodifiable and a snapshot of the information at
		/// the time of the query. The state may change after this query returns. The
		/// caller should handle the situation that some or all of these resource
		/// references are no longer relevant.
		/// </remarks>
		/// <returns>
		/// the collection that contains the resource references associated
		/// with the resource; or an empty collection if no resource references
		/// are registered under this resource
		/// </returns>
		public override ICollection<SharedCacheResourceReference> GetResourceReferences(string
			 key)
		{
			string interned = Intern(key);
			lock (interned)
			{
				SharedCacheResource resource = cachedResources[interned];
				if (resource == null)
				{
					return Sharpen.Collections.EmptySet();
				}
				ICollection<SharedCacheResourceReference> refs = new HashSet<SharedCacheResourceReference
					>(resource.GetResourceReferences());
				return Sharpen.Collections.UnmodifiableSet(refs);
			}
		}

		/// <summary>Removes the provided resource reference from the resource.</summary>
		/// <remarks>
		/// Removes the provided resource reference from the resource. If the resource
		/// does not exist, nothing will be done.
		/// </remarks>
		public override bool RemoveResourceReference(string key, SharedCacheResourceReference
			 @ref, bool updateAccessTime)
		{
			string interned = Intern(key);
			lock (interned)
			{
				bool removed = false;
				SharedCacheResource resource = cachedResources[interned];
				if (resource != null)
				{
					ICollection<SharedCacheResourceReference> resourceRefs = resource.GetResourceReferences
						();
					removed = resourceRefs.Remove(@ref);
					if (updateAccessTime)
					{
						resource.UpdateAccessTime();
					}
				}
				return removed;
			}
		}

		/// <summary>Removes the provided collection of resource references from the resource.
		/// 	</summary>
		/// <remarks>
		/// Removes the provided collection of resource references from the resource.
		/// If the resource does not exist, nothing will be done.
		/// </remarks>
		public override void RemoveResourceReferences(string key, ICollection<SharedCacheResourceReference
			> refs, bool updateAccessTime)
		{
			string interned = Intern(key);
			lock (interned)
			{
				SharedCacheResource resource = cachedResources[interned];
				if (resource != null)
				{
					ICollection<SharedCacheResourceReference> resourceRefs = resource.GetResourceReferences
						();
					resourceRefs.RemoveAll(refs);
					if (updateAccessTime)
					{
						resource.UpdateAccessTime();
					}
				}
			}
		}

		/// <summary>Provides atomicity for the method.</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void CleanResourceReferences(string key)
		{
			string interned = Intern(key);
			lock (interned)
			{
				base.CleanResourceReferences(key);
			}
		}

		/// <summary>Removes the given resource from the store.</summary>
		/// <remarks>
		/// Removes the given resource from the store. Returns true if the resource is
		/// found and removed or if the resource is not found. Returns false if it was
		/// unable to remove the resource because the resource reference list was not
		/// empty.
		/// </remarks>
		public override bool RemoveResource(string key)
		{
			string interned = Intern(key);
			lock (interned)
			{
				SharedCacheResource resource = cachedResources[interned];
				if (resource == null)
				{
					return true;
				}
				if (!resource.GetResourceReferences().IsEmpty())
				{
					return false;
				}
				// no users
				Sharpen.Collections.Remove(cachedResources, interned);
				return true;
			}
		}

		/// <summary>Obtains the access time for a resource.</summary>
		/// <remarks>
		/// Obtains the access time for a resource. It represents the view of the
		/// resource at the time of the query. The value may have been updated at a
		/// later point.
		/// </remarks>
		/// <returns>
		/// the access time of the resource if found; -1 if the resource is not
		/// found
		/// </returns>
		[VisibleForTesting]
		internal virtual long GetAccessTime(string key)
		{
			string interned = Intern(key);
			lock (interned)
			{
				SharedCacheResource resource = cachedResources[interned];
				return resource == null ? -1 : resource.GetAccessTime();
			}
		}

		public override bool IsResourceEvictable(string key, FileStatus file)
		{
			lock (initialAppsLock)
			{
				if (initialApps.Count > 0)
				{
					return false;
				}
			}
			long staleTime = Runtime.CurrentTimeMillis() - TimeUnit.Minutes.ToMillis(this.stalenessMinutes
				);
			long accessTime = GetAccessTime(key);
			if (accessTime == -1)
			{
				// check modification time
				long modTime = file.GetModificationTime();
				// if modification time is older then the store startup time, we need to
				// just use the store startup time as the last point of certainty
				long lastUse = modTime < this.startTime ? this.startTime : modTime;
				return lastUse < staleTime;
			}
			else
			{
				// check access time
				return accessTime < staleTime;
			}
		}

		private static int GetStalenessPeriod(Configuration conf)
		{
			int stalenessMinutes = conf.GetInt(YarnConfiguration.InMemoryStalenessPeriodMins, 
				YarnConfiguration.DefaultInMemoryStalenessPeriodMins);
			// non-positive value is invalid; use the default
			if (stalenessMinutes <= 0)
			{
				throw new HadoopIllegalArgumentException("Non-positive staleness value: " + stalenessMinutes
					 + ". The staleness value must be greater than zero.");
			}
			return stalenessMinutes;
		}

		private static int GetInitialDelay(Configuration conf)
		{
			int initialMinutes = conf.GetInt(YarnConfiguration.InMemoryInitialDelayMins, YarnConfiguration
				.DefaultInMemoryInitialDelayMins);
			// non-positive value is invalid; use the default
			if (initialMinutes <= 0)
			{
				throw new HadoopIllegalArgumentException("Non-positive initial delay value: " + initialMinutes
					 + ". The initial delay value must be greater than zero.");
			}
			return initialMinutes;
		}

		private static int GetCheckPeriod(Configuration conf)
		{
			int checkMinutes = conf.GetInt(YarnConfiguration.InMemoryCheckPeriodMins, YarnConfiguration
				.DefaultInMemoryCheckPeriodMins);
			// non-positive value is invalid; use the default
			if (checkMinutes <= 0)
			{
				throw new HadoopIllegalArgumentException("Non-positive check period value: " + checkMinutes
					 + ". The check period value must be greater than zero.");
			}
			return checkMinutes;
		}

		internal class AppCheckTask : Runnable
		{
			private readonly AppChecker taskAppChecker;

			public AppCheckTask(InMemorySCMStore _enclosing, AppChecker appChecker)
			{
				this._enclosing = _enclosing;
				this.taskAppChecker = appChecker;
			}

			public virtual void Run()
			{
				try
				{
					InMemorySCMStore.Log.Info("Checking the initial app list for finished applications."
						);
					lock (this._enclosing.initialAppsLock)
					{
						if (this._enclosing.initialApps.IsEmpty())
						{
						}
						else
						{
							// we're fine, no-op; there are no active apps that were running at
							// the time of the service start
							InMemorySCMStore.Log.Info("Looking into " + this._enclosing.initialApps.Count + " apps to see if they are still active"
								);
							IEnumerator<ApplicationId> it = this._enclosing.initialApps.GetEnumerator();
							while (it.HasNext())
							{
								ApplicationId id = it.Next();
								try
								{
									if (!this.taskAppChecker.IsApplicationActive(id))
									{
										// remove it from the list
										it.Remove();
									}
								}
								catch (YarnException e)
								{
									InMemorySCMStore.Log.Warn("Exception while checking the app status;" + " will leave the entry in the list"
										, e);
								}
							}
						}
						// continue
						InMemorySCMStore.Log.Info("There are now " + this._enclosing.initialApps.Count + 
							" entries in the list");
					}
				}
				catch (Exception e)
				{
					InMemorySCMStore.Log.Error("Unexpected exception thrown during in-memory store app check task."
						 + " Rescheduling task.", e);
				}
			}

			private readonly InMemorySCMStore _enclosing;
		}
	}
}

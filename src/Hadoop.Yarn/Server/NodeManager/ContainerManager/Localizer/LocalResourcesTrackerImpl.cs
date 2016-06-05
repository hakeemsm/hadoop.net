using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	/// <summary>
	/// A collection of
	/// <see cref="LocalizedResource"/>
	/// s all of same
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.LocalResourceVisibility"/>
	/// .
	/// </summary>
	internal class LocalResourcesTrackerImpl : LocalResourcesTracker
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalResourcesTrackerImpl
			));

		private const string RandomDirRegex = "-?\\d+";

		private static readonly Sharpen.Pattern RandomDirPattern = Sharpen.Pattern.Compile
			(RandomDirRegex);

		private readonly string user;

		private readonly ApplicationId appId;

		private readonly Dispatcher dispatcher;

		private readonly ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc;

		private Configuration conf;

		private readonly bool useLocalCacheDirectoryManager;

		private ConcurrentHashMap<Path, LocalCacheDirectoryManager> directoryManagers;

		private ConcurrentHashMap<LocalResourceRequest, Path> inProgressLocalResourcesMap;

		private AtomicLong uniqueNumberGenerator = new AtomicLong(9);

		private NMStateStoreService stateStore;

		public LocalResourcesTrackerImpl(string user, ApplicationId appId, Dispatcher dispatcher
			, bool useLocalCacheDirectoryManager, Configuration conf, NMStateStoreService stateStore
			)
			: this(user, appId, dispatcher, new ConcurrentHashMap<LocalResourceRequest, LocalizedResource
				>(), useLocalCacheDirectoryManager, conf, stateStore)
		{
		}

		internal LocalResourcesTrackerImpl(string user, ApplicationId appId, Dispatcher dispatcher
			, ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc, bool useLocalCacheDirectoryManager
			, Configuration conf, NMStateStoreService stateStore)
		{
			/*
			* This flag controls whether this resource tracker uses hierarchical
			* directories or not. For PRIVATE and PUBLIC resource trackers it
			* will be set whereas for APPLICATION resource tracker it would
			* be false.
			*/
			/*
			* It is used to keep track of resource into hierarchical directory
			* while it is getting downloaded. It is useful for reference counting
			* in case resource localization fails.
			*/
			/*
			* starting with 10 to accommodate 0-9 directories created as a part of
			* LocalCacheDirectoryManager. So there will be one unique number generator
			* per APPLICATION, USER and PUBLIC cache.
			*/
			this.appId = appId;
			this.user = user;
			this.dispatcher = dispatcher;
			this.localrsrc = localrsrc;
			this.useLocalCacheDirectoryManager = useLocalCacheDirectoryManager;
			if (this.useLocalCacheDirectoryManager)
			{
				directoryManagers = new ConcurrentHashMap<Path, LocalCacheDirectoryManager>();
				inProgressLocalResourcesMap = new ConcurrentHashMap<LocalResourceRequest, Path>();
			}
			this.conf = conf;
			this.stateStore = stateStore;
		}

		/*
		* Synchronizing this method for avoiding races due to multiple ResourceEvent's
		* coming to LocalResourcesTracker from Public/Private localizer and
		* Resource Localization Service.
		*/
		public virtual void Handle(ResourceEvent @event)
		{
			lock (this)
			{
				LocalResourceRequest req = @event.GetLocalResourceRequest();
				LocalizedResource rsrc = localrsrc[req];
				switch (@event.GetType())
				{
					case ResourceEventType.Localized:
					{
						if (useLocalCacheDirectoryManager)
						{
							Sharpen.Collections.Remove(inProgressLocalResourcesMap, req);
						}
						break;
					}

					case ResourceEventType.Request:
					{
						if (rsrc != null && (!IsResourcePresent(rsrc)))
						{
							Log.Info("Resource " + rsrc.GetLocalPath() + " is missing, localizing it again");
							RemoveResource(req);
							rsrc = null;
						}
						if (null == rsrc)
						{
							rsrc = new LocalizedResource(req, dispatcher);
							localrsrc[req] = rsrc;
						}
						break;
					}

					case ResourceEventType.Release:
					{
						if (null == rsrc)
						{
							// The container sent a release event on a resource which 
							// 1) Failed
							// 2) Removed for some reason (ex. disk is no longer accessible)
							ResourceReleaseEvent relEvent = (ResourceReleaseEvent)@event;
							Log.Info("Container " + relEvent.GetContainer() + " sent RELEASE event on a resource request "
								 + req + " not present in cache.");
							return;
						}
						break;
					}

					case ResourceEventType.LocalizationFailed:
					{
						/*
						* If resource localization fails then Localized resource will be
						* removed from local cache.
						*/
						RemoveResource(req);
						break;
					}

					case ResourceEventType.Recovered:
					{
						if (rsrc != null)
						{
							Log.Warn("Ignoring attempt to recover existing resource " + rsrc);
							return;
						}
						rsrc = RecoverResource(req, (ResourceRecoveredEvent)@event);
						localrsrc[req] = rsrc;
						break;
					}
				}
				if (rsrc == null)
				{
					Log.Warn("Received " + @event.GetType() + " event for request " + req + " but localized resource is missing"
						);
					return;
				}
				rsrc.Handle(@event);
				// Remove the resource if its downloading and its reference count has
				// become 0 after RELEASE. This maybe because a container was killed while
				// localizing and no other container is referring to the resource.
				// NOTE: This should NOT be done for public resources since the
				//       download is not associated with a container-specific localizer.
				if (@event.GetType() == ResourceEventType.Release)
				{
					if (rsrc.GetState() == ResourceState.Downloading && rsrc.GetRefCount() <= 0 && rsrc
						.GetRequest().GetVisibility() != LocalResourceVisibility.Public)
					{
						RemoveResource(req);
					}
				}
				if (@event.GetType() == ResourceEventType.Localized)
				{
					if (rsrc.GetLocalPath() != null)
					{
						try
						{
							stateStore.FinishResourceLocalization(user, appId, BuildLocalizedResourceProto(rsrc
								));
						}
						catch (IOException ioe)
						{
							Log.Error("Error storing resource state for " + rsrc, ioe);
						}
					}
					else
					{
						Log.Warn("Resource " + rsrc + " localized without a location");
					}
				}
			}
		}

		private LocalizedResource RecoverResource(LocalResourceRequest req, ResourceRecoveredEvent
			 @event)
		{
			// unique number for a resource is the directory of the resource
			Path localDir = @event.GetLocalPath().GetParent();
			long rsrcId = long.Parse(localDir.GetName());
			// update ID generator to avoid conflicts with existing resources
			while (true)
			{
				long currentRsrcId = uniqueNumberGenerator.Get();
				long nextRsrcId = Math.Max(currentRsrcId, rsrcId);
				if (uniqueNumberGenerator.CompareAndSet(currentRsrcId, nextRsrcId))
				{
					break;
				}
			}
			IncrementFileCountForLocalCacheDirectory(localDir.GetParent());
			return new LocalizedResource(req, dispatcher);
		}

		private YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto BuildLocalizedResourceProto
			(LocalizedResource rsrc)
		{
			return ((YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(BuildLocalResourceProto(rsrc.GetRequest())).SetLocalPath
				(rsrc.GetLocalPath().ToString()).SetSize(rsrc.GetSize()).Build());
		}

		private YarnProtos.LocalResourceProto BuildLocalResourceProto(LocalResource lr)
		{
			LocalResourcePBImpl lrpb;
			if (!(lr is LocalResourcePBImpl))
			{
				lr = LocalResource.NewInstance(lr.GetResource(), lr.GetType(), lr.GetVisibility()
					, lr.GetSize(), lr.GetTimestamp(), lr.GetPattern());
			}
			lrpb = (LocalResourcePBImpl)lr;
			return lrpb.GetProto();
		}

		public virtual void IncrementFileCountForLocalCacheDirectory(Path cacheDir)
		{
			if (useLocalCacheDirectoryManager)
			{
				Path cacheRoot = LocalCacheDirectoryManager.GetCacheDirectoryRoot(cacheDir);
				if (cacheRoot != null)
				{
					LocalCacheDirectoryManager dir = directoryManagers[cacheRoot];
					if (dir == null)
					{
						dir = new LocalCacheDirectoryManager(conf);
						LocalCacheDirectoryManager otherDir = directoryManagers.PutIfAbsent(cacheRoot, dir
							);
						if (otherDir != null)
						{
							dir = otherDir;
						}
					}
					if (cacheDir.Equals(cacheRoot))
					{
						dir.IncrementFileCountForPath(string.Empty);
					}
					else
					{
						string dirStr = cacheDir.ToUri().GetRawPath();
						string rootStr = cacheRoot.ToUri().GetRawPath();
						dir.IncrementFileCountForPath(Sharpen.Runtime.Substring(dirStr, rootStr.Length + 
							1));
					}
				}
			}
		}

		/*
		* Update the file-count statistics for a local cache-directory.
		* This will retrieve the localized path for the resource from
		* 1) inProgressRsrcMap if the resource was under localization and it
		* failed.
		* 2) LocalizedResource if the resource is already localized.
		* From this path it will identify the local directory under which the
		* resource was localized. Then rest of the path will be used to decrement
		* file count for the HierarchicalSubDirectory pointing to this relative
		* path.
		*/
		private void DecrementFileCountForLocalCacheDirectory(LocalResourceRequest req, LocalizedResource
			 rsrc)
		{
			if (useLocalCacheDirectoryManager)
			{
				Path rsrcPath = null;
				if (inProgressLocalResourcesMap.Contains(req))
				{
					// This happens when localization of a resource fails.
					rsrcPath = Sharpen.Collections.Remove(inProgressLocalResourcesMap, req);
				}
				else
				{
					if (rsrc != null && rsrc.GetLocalPath() != null)
					{
						rsrcPath = rsrc.GetLocalPath().GetParent().GetParent();
					}
				}
				if (rsrcPath != null)
				{
					Path parentPath = new Path(rsrcPath.ToUri().GetRawPath());
					while (!directoryManagers.Contains(parentPath))
					{
						parentPath = parentPath.GetParent();
						if (parentPath == null)
						{
							return;
						}
					}
					if (parentPath != null)
					{
						string parentDir = parentPath.ToUri().GetRawPath().ToString();
						LocalCacheDirectoryManager dir = directoryManagers[parentPath];
						string rsrcDir = rsrcPath.ToUri().GetRawPath();
						if (rsrcDir.Equals(parentDir))
						{
							dir.DecrementFileCountForPath(string.Empty);
						}
						else
						{
							dir.DecrementFileCountForPath(Sharpen.Runtime.Substring(rsrcDir, parentDir.Length
								 + 1));
						}
					}
				}
			}
		}

		/// <summary>
		/// This module checks if the resource which was localized is already present
		/// or not
		/// </summary>
		/// <param name="rsrc"/>
		/// <returns>true/false based on resource is present or not</returns>
		public virtual bool IsResourcePresent(LocalizedResource rsrc)
		{
			bool ret = true;
			if (rsrc.GetState() == ResourceState.Localized)
			{
				FilePath file = new FilePath(rsrc.GetLocalPath().ToUri().GetRawPath().ToString());
				if (!file.Exists())
				{
					ret = false;
				}
			}
			return ret;
		}

		public virtual bool Remove(LocalizedResource rem, DeletionService delService)
		{
			// current synchronization guaranteed by crude RLS event for cleanup
			LocalizedResource rsrc = localrsrc[rem.GetRequest()];
			if (null == rsrc)
			{
				Log.Error("Attempt to remove absent resource: " + rem.GetRequest() + " from " + GetUser
					());
				return true;
			}
			if (rsrc.GetRefCount() > 0 || ResourceState.Downloading.Equals(rsrc.GetState()) ||
				 rsrc != rem)
			{
				// internal error
				Log.Error("Attempt to remove resource: " + rsrc + " with non-zero refcount");
				return false;
			}
			else
			{
				// ResourceState is LOCALIZED or INIT
				if (ResourceState.Localized.Equals(rsrc.GetState()))
				{
					delService.Delete(GetUser(), GetPathToDelete(rsrc.GetLocalPath()));
				}
				RemoveResource(rem.GetRequest());
				Log.Info("Removed " + rsrc.GetLocalPath() + " from localized cache");
				return true;
			}
		}

		private void RemoveResource(LocalResourceRequest req)
		{
			LocalizedResource rsrc = Sharpen.Collections.Remove(localrsrc, req);
			DecrementFileCountForLocalCacheDirectory(req, rsrc);
			if (rsrc != null)
			{
				Path localPath = rsrc.GetLocalPath();
				if (localPath != null)
				{
					try
					{
						stateStore.RemoveLocalizedResource(user, appId, localPath);
					}
					catch (IOException e)
					{
						Log.Error("Unable to remove resource " + rsrc + " from state store", e);
					}
				}
			}
		}

		/// <summary>Returns the path up to the random directory component.</summary>
		private Path GetPathToDelete(Path localPath)
		{
			Path delPath = localPath.GetParent();
			string name = delPath.GetName();
			Matcher matcher = RandomDirPattern.Matcher(name);
			if (matcher.Matches())
			{
				return delPath;
			}
			else
			{
				Log.Warn("Random directory component did not match. " + "Deleting localized path only"
					);
				return localPath;
			}
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual IEnumerator<LocalizedResource> GetEnumerator()
		{
			return localrsrc.Values.GetEnumerator();
		}

		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// absolute path for localization which includes local
		/// directory path and the relative hierarchical path (if use local
		/// cache directory manager is enabled)
		/// </returns>
		/// <?/>
		/// <?/>
		/// <?/>
		public virtual Path GetPathForLocalization(LocalResourceRequest req, Path localDirPath
			, DeletionService delService)
		{
			Path rPath = localDirPath;
			if (useLocalCacheDirectoryManager && localDirPath != null)
			{
				if (!directoryManagers.Contains(localDirPath))
				{
					directoryManagers.PutIfAbsent(localDirPath, new LocalCacheDirectoryManager(conf));
				}
				LocalCacheDirectoryManager dir = directoryManagers[localDirPath];
				rPath = localDirPath;
				string hierarchicalPath = dir.GetRelativePathForLocalization();
				// For most of the scenarios we will get root path only which
				// is an empty string
				if (!hierarchicalPath.IsEmpty())
				{
					rPath = new Path(localDirPath, hierarchicalPath);
				}
				inProgressLocalResourcesMap[req] = rPath;
			}
			while (true)
			{
				Path uniquePath = new Path(rPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet
					()));
				FilePath file = new FilePath(uniquePath.ToUri().GetRawPath());
				if (!file.Exists())
				{
					rPath = uniquePath;
					break;
				}
				// If the directory already exists, delete it and move to next one.
				Log.Warn("Directory " + uniquePath + " already exists, " + "try next one.");
				if (delService != null)
				{
					delService.Delete(GetUser(), uniquePath);
				}
			}
			Path localPath = new Path(rPath, req.GetPath().GetName());
			LocalizedResource rsrc = localrsrc[req];
			rsrc.SetLocalPath(localPath);
			LocalResource lr = LocalResource.NewInstance(req.GetResource(), req.GetType(), req
				.GetVisibility(), req.GetSize(), req.GetTimestamp());
			try
			{
				stateStore.StartResourceLocalization(user, appId, ((LocalResourcePBImpl)lr).GetProto
					(), localPath);
			}
			catch (IOException e)
			{
				Log.Error("Unable to record localization start for " + rsrc, e);
			}
			return rPath;
		}

		public virtual LocalizedResource GetLocalizedResource(LocalResourceRequest request
			)
		{
			return localrsrc[request];
		}

		[VisibleForTesting]
		internal virtual LocalCacheDirectoryManager GetDirectoryManager(Path localDirPath
			)
		{
			LocalCacheDirectoryManager mgr = null;
			if (useLocalCacheDirectoryManager)
			{
				mgr = directoryManagers[localDirPath];
			}
			return mgr;
		}
	}
}

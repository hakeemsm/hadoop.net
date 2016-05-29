using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store
{
	/// <summary>
	/// An abstract class for the data store used by the shared cache manager
	/// service.
	/// </summary>
	/// <remarks>
	/// An abstract class for the data store used by the shared cache manager
	/// service. All implementations of methods in this interface need to be thread
	/// safe and atomic.
	/// </remarks>
	public abstract class SCMStore : CompositeService
	{
		protected internal AppChecker appChecker;

		protected internal SCMStore(string name)
			: base(name)
		{
		}

		[VisibleForTesting]
		internal SCMStore(string name, AppChecker appChecker)
			: base(name)
		{
			this.appChecker = appChecker;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			if (this.appChecker == null)
			{
				this.appChecker = CreateAppCheckerService(conf);
			}
			AddService(appChecker);
			base.ServiceInit(conf);
		}

		/// <summary>Add a resource to the shared cache and it's associated filename.</summary>
		/// <remarks>
		/// Add a resource to the shared cache and it's associated filename. The
		/// resource is identified by a unique key. If the key already exists no action
		/// is taken and the filename of the existing resource is returned. If the key
		/// does not exist, the resource is added, it's access time is set, and the
		/// filename of the resource is returned.
		/// </remarks>
		/// <param name="key">a unique identifier for a resource</param>
		/// <param name="fileName">the filename of the resource</param>
		/// <returns>the filename of the resource as represented by the cache</returns>
		[InterfaceAudience.Private]
		public abstract string AddResource(string key, string fileName);

		/// <summary>Remove a resource from the shared cache.</summary>
		/// <param name="key">a unique identifier for a resource</param>
		/// <returns>
		/// true if the resource was removed or did not exist, false if the
		/// resource existed, contained at least one
		/// <code>SharedCacheResourceReference</code> and was not removed.
		/// </returns>
		[InterfaceAudience.Private]
		public abstract bool RemoveResource(string key);

		/// <summary>
		/// Add a <code>SharedCacheResourceReference</code> to a resource and update
		/// the resource access time.
		/// </summary>
		/// <param name="key">a unique identifier for a resource</param>
		/// <param name="ref">the <code>SharedCacheResourceReference</code> to add</param>
		/// <returns>
		/// String the filename of the resource if the
		/// <code>SharedCacheResourceReference</code> was added or already
		/// existed. null if the resource did not exist
		/// </returns>
		[InterfaceAudience.Private]
		public abstract string AddResourceReference(string key, SharedCacheResourceReference
			 @ref);

		/// <summary>
		/// Get the <code>SharedCacheResourceReference</code>(s) associated with the
		/// resource.
		/// </summary>
		/// <param name="key">a unique identifier for a resource</param>
		/// <returns>
		/// an unmodifiable collection of
		/// <code>SharedCacheResourceReferences</code>. If the resource does
		/// not exist, an empty set is returned.
		/// </returns>
		[InterfaceAudience.Private]
		public abstract ICollection<SharedCacheResourceReference> GetResourceReferences(string
			 key);

		/// <summary>Remove a <code>SharedCacheResourceReference</code> from a resource.</summary>
		/// <param name="key">a unique identifier for a resource</param>
		/// <param name="ref">the <code>SharedCacheResourceReference</code> to remove</param>
		/// <param name="updateAccessTime">
		/// true if the call should update the access time for
		/// the resource
		/// </param>
		/// <returns>true if the reference was removed, false otherwise</returns>
		[InterfaceAudience.Private]
		public abstract bool RemoveResourceReference(string key, SharedCacheResourceReference
			 @ref, bool updateAccessTime);

		/// <summary>
		/// Remove a collection of <code>SharedCacheResourceReferences</code> from a
		/// resource.
		/// </summary>
		/// <param name="key">a unique identifier for a resource</param>
		/// <param name="refs">
		/// the collection of <code>SharedCacheResourceReference</code>s to
		/// remove
		/// </param>
		/// <param name="updateAccessTime">
		/// true if the call should update the access time for
		/// the resource
		/// </param>
		[InterfaceAudience.Private]
		public abstract void RemoveResourceReferences(string key, ICollection<SharedCacheResourceReference
			> refs, bool updateAccessTime);

		/// <summary>
		/// Clean all resource references to a cache resource that contain application
		/// ids pointing to finished applications.
		/// </summary>
		/// <remarks>
		/// Clean all resource references to a cache resource that contain application
		/// ids pointing to finished applications. If the resource key does not exist,
		/// do nothing.
		/// </remarks>
		/// <param name="key">a unique identifier for a resource</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		public virtual void CleanResourceReferences(string key)
		{
			ICollection<SharedCacheResourceReference> refs = GetResourceReferences(key);
			if (!refs.IsEmpty())
			{
				ICollection<SharedCacheResourceReference> refsToRemove = new HashSet<SharedCacheResourceReference
					>();
				foreach (SharedCacheResourceReference r in refs)
				{
					if (!appChecker.IsApplicationActive(r.GetAppId()))
					{
						// application in resource reference is dead, it is safe to remove the
						// reference
						refsToRemove.AddItem(r);
					}
				}
				if (refsToRemove.Count > 0)
				{
					RemoveResourceReferences(key, refsToRemove, false);
				}
			}
		}

		/// <summary>
		/// Check if a specific resource is evictable according to the store's enabled
		/// cache eviction policies.
		/// </summary>
		/// <param name="key">a unique identifier for a resource</param>
		/// <param name="file">
		/// the <code>FileStatus</code> object for the resource file in the
		/// file system.
		/// </param>
		/// <returns>true if the resource is evicatble, false otherwise</returns>
		[InterfaceAudience.Private]
		public abstract bool IsResourceEvictable(string key, FileStatus file);

		/// <summary>
		/// Create an instance of the AppChecker service via reflection based on the
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.ScmAppCheckerClass"/>
		/// parameter.
		/// </summary>
		/// <param name="conf"/>
		/// <returns>an instance of the AppChecker class</returns>
		[InterfaceAudience.Private]
		public static AppChecker CreateAppCheckerService(Configuration conf)
		{
			Type defaultCheckerClass;
			try
			{
				defaultCheckerClass = (Type)Sharpen.Runtime.GetType(YarnConfiguration.DefaultScmAppCheckerClass
					);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException("Invalid default scm app checker class" + YarnConfiguration
					.DefaultScmAppCheckerClass, e);
			}
			AppChecker checker = ReflectionUtils.NewInstance(conf.GetClass<AppChecker>(YarnConfiguration
				.ScmAppCheckerClass, defaultCheckerClass), conf);
			return checker;
		}
	}
}

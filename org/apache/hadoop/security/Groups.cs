using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>A user-to-groups mapping service.</summary>
	/// <remarks>
	/// A user-to-groups mapping service.
	/// <see cref="Groups"/>
	/// allows for server to get the various group memberships
	/// of a given user via the
	/// <see cref="getGroups(string)"/>
	/// call, thus ensuring
	/// a consistent user-to-groups mapping and protects against vagaries of
	/// different mappings on servers and clients in a Hadoop cluster.
	/// </remarks>
	public class Groups
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.Groups
			)));

		private readonly org.apache.hadoop.security.GroupMappingServiceProvider impl;

		private readonly com.google.common.cache.LoadingCache<string, System.Collections.Generic.IList
			<string>> cache;

		private readonly System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
			<string>> staticUserToGroupsMap = new System.Collections.Generic.Dictionary<string
			, System.Collections.Generic.IList<string>>();

		private readonly long cacheTimeout;

		private readonly long negativeCacheTimeout;

		private readonly long warningDeltaMs;

		private readonly org.apache.hadoop.util.Timer timer;

		private System.Collections.Generic.ICollection<string> negativeCache;

		public Groups(org.apache.hadoop.conf.Configuration conf)
			: this(conf, new org.apache.hadoop.util.Timer())
		{
		}

		public Groups(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.util.Timer
			 timer)
		{
			impl = org.apache.hadoop.util.ReflectionUtils.newInstance(conf.getClass<org.apache.hadoop.security.GroupMappingServiceProvider
				>(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.ShellBasedUnixGroupsMapping))), conf);
			cacheTimeout = conf.getLong(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS
				, org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT
				) * 1000;
			negativeCacheTimeout = conf.getLong(org.apache.hadoop.fs.CommonConfigurationKeys.
				HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, org.apache.hadoop.fs.CommonConfigurationKeys
				.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS_DEFAULT) * 1000;
			warningDeltaMs = conf.getLong(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS
				, org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS_DEFAULT
				);
			parseStaticMapping(conf);
			this.timer = timer;
			this.cache = com.google.common.cache.CacheBuilder.newBuilder().refreshAfterWrite(
				cacheTimeout, java.util.concurrent.TimeUnit.MILLISECONDS).ticker(new org.apache.hadoop.security.Groups.TimerToTickerAdapter
				(timer)).expireAfterWrite(10 * cacheTimeout, java.util.concurrent.TimeUnit.MILLISECONDS
				).build(new org.apache.hadoop.security.Groups.GroupCacheLoader(this));
			if (negativeCacheTimeout > 0)
			{
				com.google.common.cache.Cache<string, bool> tempMap = com.google.common.cache.CacheBuilder
					.newBuilder().expireAfterWrite(negativeCacheTimeout, java.util.concurrent.TimeUnit
					.MILLISECONDS).ticker(new org.apache.hadoop.security.Groups.TimerToTickerAdapter
					(timer)).build();
				negativeCache = java.util.Collections.newSetFromMap(tempMap.asMap());
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Group mapping impl=" + Sharpen.Runtime.getClassForObject(impl).getName
					() + "; cacheTimeout=" + cacheTimeout + "; warningDeltaMs=" + warningDeltaMs);
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual System.Collections.Generic.ICollection<string> getNegativeCache(
			)
		{
			return negativeCache;
		}

		/*
		* Parse the hadoop.user.group.static.mapping.overrides configuration to
		* staticUserToGroupsMap
		*/
		private void parseStaticMapping(org.apache.hadoop.conf.Configuration conf)
		{
			string staticMapping = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES
				, org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT
				);
			System.Collections.Generic.ICollection<string> mappings = org.apache.hadoop.util.StringUtils
				.getStringCollection(staticMapping, ";");
			foreach (string users in mappings)
			{
				System.Collections.Generic.ICollection<string> userToGroups = org.apache.hadoop.util.StringUtils
					.getStringCollection(users, "=");
				if (userToGroups.Count < 1 || userToGroups.Count > 2)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Configuration " + org.apache.hadoop.fs.CommonConfigurationKeys
						.HADOOP_USER_GROUP_STATIC_OVERRIDES + " is invalid");
				}
				string[] userToGroupsArray = Sharpen.Collections.ToArray(userToGroups, new string
					[userToGroups.Count]);
				string user = userToGroupsArray[0];
				System.Collections.Generic.IList<string> groups = java.util.Collections.emptyList
					();
				if (userToGroupsArray.Length == 2)
				{
					groups = (System.Collections.Generic.IList<string>)org.apache.hadoop.util.StringUtils
						.getStringCollection(userToGroupsArray[1]);
				}
				staticUserToGroupsMap[user] = groups;
			}
		}

		private bool isNegativeCacheEnabled()
		{
			return negativeCacheTimeout > 0;
		}

		private System.IO.IOException noGroupsForUser(string user)
		{
			return new System.IO.IOException("No groups found for user " + user);
		}

		/// <summary>Get the group memberships of a given user.</summary>
		/// <remarks>
		/// Get the group memberships of a given user.
		/// If the user's group is not cached, this method may block.
		/// </remarks>
		/// <param name="user">User's name</param>
		/// <returns>the group memberships of the user</returns>
		/// <exception cref="System.IO.IOException">if user does not exist</exception>
		public virtual System.Collections.Generic.IList<string> getGroups(string user)
		{
			// No need to lookup for groups of static users
			System.Collections.Generic.IList<string> staticMapping = staticUserToGroupsMap[user
				];
			if (staticMapping != null)
			{
				return staticMapping;
			}
			// Check the negative cache first
			if (isNegativeCacheEnabled())
			{
				if (negativeCache.contains(user))
				{
					throw noGroupsForUser(user);
				}
			}
			try
			{
				return cache.get(user);
			}
			catch (java.util.concurrent.ExecutionException e)
			{
				throw (System.IO.IOException)e.InnerException;
			}
		}

		/// <summary>Convert millisecond times from hadoop's timer to guava's nanosecond ticker.
		/// 	</summary>
		private class TimerToTickerAdapter : com.google.common.@base.Ticker
		{
			private org.apache.hadoop.util.Timer timer;

			public TimerToTickerAdapter(org.apache.hadoop.util.Timer timer)
			{
				this.timer = timer;
			}

			public override long read()
			{
				long NANOSECONDS_PER_MS = 1000000;
				return timer.monotonicNow() * NANOSECONDS_PER_MS;
			}
		}

		/// <summary>Deals with loading data into the cache.</summary>
		private class GroupCacheLoader : com.google.common.cache.CacheLoader<string, System.Collections.Generic.IList
			<string>>
		{
			/// <summary>
			/// This method will block if a cache entry doesn't exist, and
			/// any subsequent requests for the same user will wait on this
			/// request to return.
			/// </summary>
			/// <remarks>
			/// This method will block if a cache entry doesn't exist, and
			/// any subsequent requests for the same user will wait on this
			/// request to return. If a user already exists in the cache,
			/// this will be run in the background.
			/// </remarks>
			/// <param name="user">key of cache</param>
			/// <returns>List of groups belonging to user</returns>
			/// <exception cref="System.IO.IOException">to prevent caching negative entries</exception>
			/// <exception cref="System.Exception"/>
			public override System.Collections.Generic.IList<string> load(string user)
			{
				System.Collections.Generic.IList<string> groups = this.fetchGroupList(user);
				if (groups.isEmpty())
				{
					if (this._enclosing.isNegativeCacheEnabled())
					{
						this._enclosing.negativeCache.add(user);
					}
					// We throw here to prevent Cache from retaining an empty group
					throw this._enclosing.noGroupsForUser(user);
				}
				return groups;
			}

			/// <summary>Queries impl for groups belonging to the user.</summary>
			/// <remarks>Queries impl for groups belonging to the user. This could involve I/O and take awhile.
			/// 	</remarks>
			/// <exception cref="System.IO.IOException"/>
			private System.Collections.Generic.IList<string> fetchGroupList(string user)
			{
				long startMs = this._enclosing.timer.monotonicNow();
				System.Collections.Generic.IList<string> groupList = this._enclosing.impl.getGroups
					(user);
				long endMs = this._enclosing.timer.monotonicNow();
				long deltaMs = endMs - startMs;
				org.apache.hadoop.security.UserGroupInformation.metrics.addGetGroups(deltaMs);
				if (deltaMs > this._enclosing.warningDeltaMs)
				{
					org.apache.hadoop.security.Groups.LOG.warn("Potential performance problem: getGroups(user="
						 + user + ") " + "took " + deltaMs + " milliseconds.");
				}
				return groupList;
			}

			internal GroupCacheLoader(Groups _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly Groups _enclosing;
		}

		/// <summary>Refresh all user-to-groups mappings.</summary>
		public virtual void refresh()
		{
			LOG.info("clearing userToGroupsMap cache");
			try
			{
				impl.cacheGroupsRefresh();
			}
			catch (System.IO.IOException e)
			{
				LOG.warn("Error refreshing groups cache", e);
			}
			cache.invalidateAll();
			if (isNegativeCacheEnabled())
			{
				negativeCache.clear();
			}
		}

		/// <summary>Add groups to cache</summary>
		/// <param name="groups">list of groups to add to cache</param>
		public virtual void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			)
		{
			try
			{
				impl.cacheGroupsAdd(groups);
			}
			catch (System.IO.IOException e)
			{
				LOG.warn("Error caching groups", e);
			}
		}

		private static org.apache.hadoop.security.Groups GROUPS = null;

		/// <summary>Get the groups being used to map user-to-groups.</summary>
		/// <returns>the groups being used to map user-to-groups.</returns>
		public static org.apache.hadoop.security.Groups getUserToGroupsMappingService()
		{
			return getUserToGroupsMappingService(new org.apache.hadoop.conf.Configuration());
		}

		/// <summary>Get the groups being used to map user-to-groups.</summary>
		/// <param name="conf"/>
		/// <returns>the groups being used to map user-to-groups.</returns>
		public static org.apache.hadoop.security.Groups getUserToGroupsMappingService(org.apache.hadoop.conf.Configuration
			 conf)
		{
			lock (typeof(Groups))
			{
				if (GROUPS == null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug(" Creating new Groups object");
					}
					GROUPS = new org.apache.hadoop.security.Groups(conf);
				}
				return GROUPS;
			}
		}

		/// <summary>Create new groups used to map user-to-groups with loaded configuration.</summary>
		/// <param name="conf"/>
		/// <returns>the groups being used to map user-to-groups.</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static org.apache.hadoop.security.Groups getUserToGroupsMappingServiceWithLoadedConfiguration
			(org.apache.hadoop.conf.Configuration conf)
		{
			lock (typeof(Groups))
			{
				GROUPS = new org.apache.hadoop.security.Groups(conf);
				return GROUPS;
			}
		}
	}
}

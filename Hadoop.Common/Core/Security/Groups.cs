using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>A user-to-groups mapping service.</summary>
	/// <remarks>
	/// A user-to-groups mapping service.
	/// <see cref="Groups"/>
	/// allows for server to get the various group memberships
	/// of a given user via the
	/// <see cref="GetGroups(string)"/>
	/// call, thus ensuring
	/// a consistent user-to-groups mapping and protects against vagaries of
	/// different mappings on servers and clients in a Hadoop cluster.
	/// </remarks>
	public class Groups
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Groups
			));

		private readonly GroupMappingServiceProvider impl;

		private readonly LoadingCache<string, IList<string>> cache;

		private readonly IDictionary<string, IList<string>> staticUserToGroupsMap = new Dictionary
			<string, IList<string>>();

		private readonly long cacheTimeout;

		private readonly long negativeCacheTimeout;

		private readonly long warningDeltaMs;

		private readonly Timer timer;

		private ICollection<string> negativeCache;

		public Groups(Configuration conf)
			: this(conf, new Timer())
		{
		}

		public Groups(Configuration conf, Timer timer)
		{
			impl = ReflectionUtils.NewInstance(conf.GetClass<GroupMappingServiceProvider>(CommonConfigurationKeys
				.HadoopSecurityGroupMapping, typeof(ShellBasedUnixGroupsMapping)), conf);
			cacheTimeout = conf.GetLong(CommonConfigurationKeys.HadoopSecurityGroupsCacheSecs
				, CommonConfigurationKeys.HadoopSecurityGroupsCacheSecsDefault) * 1000;
			negativeCacheTimeout = conf.GetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs
				, CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecsDefault) * 1000;
			warningDeltaMs = conf.GetLong(CommonConfigurationKeys.HadoopSecurityGroupsCacheWarnAfterMs
				, CommonConfigurationKeys.HadoopSecurityGroupsCacheWarnAfterMsDefault);
			ParseStaticMapping(conf);
			this.timer = timer;
			this.cache = CacheBuilder.NewBuilder().RefreshAfterWrite(cacheTimeout, TimeUnit.Milliseconds
				).Ticker(new Groups.TimerToTickerAdapter(timer)).ExpireAfterWrite(10 * cacheTimeout
				, TimeUnit.Milliseconds).Build(new Groups.GroupCacheLoader(this));
			if (negativeCacheTimeout > 0)
			{
				Com.Google.Common.Cache.Cache<string, bool> tempMap = CacheBuilder.NewBuilder().ExpireAfterWrite
					(negativeCacheTimeout, TimeUnit.Milliseconds).Ticker(new Groups.TimerToTickerAdapter
					(timer)).Build();
				negativeCache = Sharpen.Collections.NewSetFromMap(tempMap.AsMap());
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Group mapping impl=" + impl.GetType().FullName + "; cacheTimeout=" + cacheTimeout
					 + "; warningDeltaMs=" + warningDeltaMs);
			}
		}

		[VisibleForTesting]
		internal virtual ICollection<string> GetNegativeCache()
		{
			return negativeCache;
		}

		/*
		* Parse the hadoop.user.group.static.mapping.overrides configuration to
		* staticUserToGroupsMap
		*/
		private void ParseStaticMapping(Configuration conf)
		{
			string staticMapping = conf.Get(CommonConfigurationKeys.HadoopUserGroupStaticOverrides
				, CommonConfigurationKeys.HadoopUserGroupStaticOverridesDefault);
			ICollection<string> mappings = StringUtils.GetStringCollection(staticMapping, ";"
				);
			foreach (string users in mappings)
			{
				ICollection<string> userToGroups = StringUtils.GetStringCollection(users, "=");
				if (userToGroups.Count < 1 || userToGroups.Count > 2)
				{
					throw new HadoopIllegalArgumentException("Configuration " + CommonConfigurationKeys
						.HadoopUserGroupStaticOverrides + " is invalid");
				}
				string[] userToGroupsArray = Sharpen.Collections.ToArray(userToGroups, new string
					[userToGroups.Count]);
				string user = userToGroupsArray[0];
				IList<string> groups = Sharpen.Collections.EmptyList();
				if (userToGroupsArray.Length == 2)
				{
					groups = (IList<string>)StringUtils.GetStringCollection(userToGroupsArray[1]);
				}
				staticUserToGroupsMap[user] = groups;
			}
		}

		private bool IsNegativeCacheEnabled()
		{
			return negativeCacheTimeout > 0;
		}

		private IOException NoGroupsForUser(string user)
		{
			return new IOException("No groups found for user " + user);
		}

		/// <summary>Get the group memberships of a given user.</summary>
		/// <remarks>
		/// Get the group memberships of a given user.
		/// If the user's group is not cached, this method may block.
		/// </remarks>
		/// <param name="user">User's name</param>
		/// <returns>the group memberships of the user</returns>
		/// <exception cref="System.IO.IOException">if user does not exist</exception>
		public virtual IList<string> GetGroups(string user)
		{
			// No need to lookup for groups of static users
			IList<string> staticMapping = staticUserToGroupsMap[user];
			if (staticMapping != null)
			{
				return staticMapping;
			}
			// Check the negative cache first
			if (IsNegativeCacheEnabled())
			{
				if (negativeCache.Contains(user))
				{
					throw NoGroupsForUser(user);
				}
			}
			try
			{
				return cache.Get(user);
			}
			catch (ExecutionException e)
			{
				throw (IOException)e.InnerException;
			}
		}

		/// <summary>Convert millisecond times from hadoop's timer to guava's nanosecond ticker.
		/// 	</summary>
		private class TimerToTickerAdapter : Ticker
		{
			private Timer timer;

			public TimerToTickerAdapter(Timer timer)
			{
				this.timer = timer;
			}

			public override long Read()
			{
				long NanosecondsPerMs = 1000000;
				return timer.MonotonicNow() * NanosecondsPerMs;
			}
		}

		/// <summary>Deals with loading data into the cache.</summary>
		private class GroupCacheLoader : CacheLoader<string, IList<string>>
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
			public override IList<string> Load(string user)
			{
				IList<string> groups = this.FetchGroupList(user);
				if (groups.IsEmpty())
				{
					if (this._enclosing.IsNegativeCacheEnabled())
					{
						this._enclosing.negativeCache.AddItem(user);
					}
					// We throw here to prevent Cache from retaining an empty group
					throw this._enclosing.NoGroupsForUser(user);
				}
				return groups;
			}

			/// <summary>Queries impl for groups belonging to the user.</summary>
			/// <remarks>Queries impl for groups belonging to the user. This could involve I/O and take awhile.
			/// 	</remarks>
			/// <exception cref="System.IO.IOException"/>
			private IList<string> FetchGroupList(string user)
			{
				long startMs = this._enclosing.timer.MonotonicNow();
				IList<string> groupList = this._enclosing.impl.GetGroups(user);
				long endMs = this._enclosing.timer.MonotonicNow();
				long deltaMs = endMs - startMs;
				UserGroupInformation.metrics.AddGetGroups(deltaMs);
				if (deltaMs > this._enclosing.warningDeltaMs)
				{
					Groups.Log.Warn("Potential performance problem: getGroups(user=" + user + ") " + 
						"took " + deltaMs + " milliseconds.");
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
		public virtual void Refresh()
		{
			Log.Info("clearing userToGroupsMap cache");
			try
			{
				impl.CacheGroupsRefresh();
			}
			catch (IOException e)
			{
				Log.Warn("Error refreshing groups cache", e);
			}
			cache.InvalidateAll();
			if (IsNegativeCacheEnabled())
			{
				negativeCache.Clear();
			}
		}

		/// <summary>Add groups to cache</summary>
		/// <param name="groups">list of groups to add to cache</param>
		public virtual void CacheGroupsAdd(IList<string> groups)
		{
			try
			{
				impl.CacheGroupsAdd(groups);
			}
			catch (IOException e)
			{
				Log.Warn("Error caching groups", e);
			}
		}

		private static Groups Groups = null;

		/// <summary>Get the groups being used to map user-to-groups.</summary>
		/// <returns>the groups being used to map user-to-groups.</returns>
		public static Groups GetUserToGroupsMappingService()
		{
			return GetUserToGroupsMappingService(new Configuration());
		}

		/// <summary>Get the groups being used to map user-to-groups.</summary>
		/// <param name="conf"/>
		/// <returns>the groups being used to map user-to-groups.</returns>
		public static Groups GetUserToGroupsMappingService(Configuration conf)
		{
			lock (typeof(Groups))
			{
				if (Groups == null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(" Creating new Groups object");
					}
					Groups = new Groups(conf);
				}
				return Groups;
			}
		}

		/// <summary>Create new groups used to map user-to-groups with loaded configuration.</summary>
		/// <param name="conf"/>
		/// <returns>the groups being used to map user-to-groups.</returns>
		[InterfaceAudience.Private]
		public static Groups GetUserToGroupsMappingServiceWithLoadedConfiguration(Configuration
			 conf)
		{
			lock (typeof(Groups))
			{
				Groups = new Groups(conf);
				return Groups;
			}
		}
	}
}

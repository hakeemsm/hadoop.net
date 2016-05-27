using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A JNI-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that invokes libC calls to get the group
	/// memberships of a given user.
	/// </summary>
	public class JniBasedUnixGroupsNetgroupMapping : JniBasedUnixGroupsMapping
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JniBasedUnixGroupsNetgroupMapping
			));

		internal virtual string[] GetUsersForNetgroupJNI(string group)
		{
		}

		static JniBasedUnixGroupsNetgroupMapping()
		{
			if (!NativeCodeLoader.IsNativeCodeLoaded())
			{
				throw new RuntimeException("Bailing out since native library couldn't " + "be loaded"
					);
			}
			Log.Debug("Using JniBasedUnixGroupsNetgroupMapping for Netgroup resolution");
		}

		/// <summary>Gets unix groups and netgroups for the user.</summary>
		/// <remarks>
		/// Gets unix groups and netgroups for the user.
		/// It gets all unix groups as returned by id -Gn but it
		/// only returns netgroups that are used in ACLs (there is
		/// no way to get all netgroups for a given user, see
		/// documentation for getent netgroup)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetGroups(string user)
		{
			// parent gets unix groups
			IList<string> groups = new List<string>(base.GetGroups(user));
			NetgroupCache.GetNetgroups(user, groups);
			return groups;
		}

		/// <summary>Refresh the netgroup cache</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsRefresh()
		{
			IList<string> groups = NetgroupCache.GetNetgroupNames();
			NetgroupCache.Clear();
			CacheGroupsAdd(groups);
		}

		/// <summary>Add a group to cache, only netgroups are cached</summary>
		/// <param name="groups">list of group names to add to cache</param>
		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsAdd(IList<string> groups)
		{
			foreach (string group in groups)
			{
				if (group.Length == 0)
				{
				}
				else
				{
					// better safe than sorry (should never happen)
					if (group[0] == '@')
					{
						if (!NetgroupCache.IsCached(group))
						{
							NetgroupCache.Add(group, GetUsersForNetgroup(group));
						}
					}
				}
			}
		}

		// unix group, not caching
		/// <summary>
		/// Calls JNI function to get users for a netgroup, since C functions
		/// are not reentrant we need to make this synchronized (see
		/// documentation for setnetgrent, getnetgrent and endnetgrent)
		/// </summary>
		/// <param name="netgroup">return users for this netgroup</param>
		/// <returns>list of users for a given netgroup</returns>
		protected internal virtual IList<string> GetUsersForNetgroup(string netgroup)
		{
			lock (this)
			{
				string[] users = null;
				try
				{
					// JNI code does not expect '@' at the begining of the group name
					users = GetUsersForNetgroupJNI(Sharpen.Runtime.Substring(netgroup, 1));
				}
				catch (Exception e)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Error getting users for netgroup " + netgroup, e);
					}
					else
					{
						Log.Info("Error getting users for netgroup " + netgroup + ": " + e.Message);
					}
				}
				if (users != null && users.Length != 0)
				{
					return Arrays.AsList(users);
				}
				return new List<string>();
			}
		}
	}
}

using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// A JNI-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that invokes libC calls to get the group
	/// memberships of a given user.
	/// </summary>
	public class JniBasedUnixGroupsNetgroupMapping : org.apache.hadoop.security.JniBasedUnixGroupsMapping
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMapping
			)));

		internal virtual string[] getUsersForNetgroupJNI(string group)
		{
		}

		static JniBasedUnixGroupsNetgroupMapping()
		{
			if (!org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				throw new System.Exception("Bailing out since native library couldn't " + "be loaded"
					);
			}
			LOG.debug("Using JniBasedUnixGroupsNetgroupMapping for Netgroup resolution");
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
		public override System.Collections.Generic.IList<string> getGroups(string user)
		{
			// parent gets unix groups
			System.Collections.Generic.IList<string> groups = new System.Collections.Generic.LinkedList
				<string>(base.getGroups(user));
			org.apache.hadoop.security.NetgroupCache.getNetgroups(user, groups);
			return groups;
		}

		/// <summary>Refresh the netgroup cache</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsRefresh()
		{
			System.Collections.Generic.IList<string> groups = org.apache.hadoop.security.NetgroupCache
				.getNetgroupNames();
			org.apache.hadoop.security.NetgroupCache.clear();
			cacheGroupsAdd(groups);
		}

		/// <summary>Add a group to cache, only netgroups are cached</summary>
		/// <param name="groups">list of group names to add to cache</param>
		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			)
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
						if (!org.apache.hadoop.security.NetgroupCache.isCached(group))
						{
							org.apache.hadoop.security.NetgroupCache.add(group, getUsersForNetgroup(group));
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
		protected internal virtual System.Collections.Generic.IList<string> getUsersForNetgroup
			(string netgroup)
		{
			lock (this)
			{
				string[] users = null;
				try
				{
					// JNI code does not expect '@' at the begining of the group name
					users = getUsersForNetgroupJNI(Sharpen.Runtime.substring(netgroup, 1));
				}
				catch (System.Exception e)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Error getting users for netgroup " + netgroup, e);
					}
					else
					{
						LOG.info("Error getting users for netgroup " + netgroup + ": " + e.Message);
					}
				}
				if (users != null && users.Length != 0)
				{
					return java.util.Arrays.asList(users);
				}
				return new System.Collections.Generic.LinkedList<string>();
			}
		}
	}
}

using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// A simple shell-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that exec's the <code>groups</code> shell command to fetch the group
	/// memberships of a given user.
	/// </summary>
	public class ShellBasedUnixGroupsNetgroupMapping : org.apache.hadoop.security.ShellBasedUnixGroupsMapping
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping
			)));

		/// <summary>Get unix groups (parent) and netgroups for given user</summary>
		/// <param name="user">get groups and netgroups for this user</param>
		/// <returns>groups and netgroups for user</returns>
		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getGroups(string user)
		{
			// parent get unix groups
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
		/// <summary>Gets users for a netgroup</summary>
		/// <param name="netgroup">return users for this netgroup</param>
		/// <returns>list of users for a given netgroup</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual System.Collections.Generic.IList<string> getUsersForNetgroup
			(string netgroup)
		{
			System.Collections.Generic.IList<string> users = new System.Collections.Generic.LinkedList
				<string>();
			// returns a string similar to this:
			// group               ( , user, ) ( domain, user1, host.com )
			string usersRaw = execShellGetUserForNetgroup(netgroup);
			// get rid of spaces, makes splitting much easier
			usersRaw = usersRaw.replaceAll(" +", string.Empty);
			// remove netgroup name at the beginning of the string
			usersRaw = usersRaw.replaceFirst(netgroup.replaceFirst("@", string.Empty) + "[()]+"
				, string.Empty);
			// split string into user infos
			string[] userInfos = usersRaw.split("[()]+");
			foreach (string userInfo in userInfos)
			{
				// userInfo: xxx,user,yyy (xxx, yyy can be empty strings)
				// get rid of everything before first and after last comma
				string user = userInfo.replaceFirst("[^,]*,", string.Empty);
				user = user.replaceFirst(",.*$", string.Empty);
				// voila! got username!
				users.add(user);
			}
			return users;
		}

		/// <summary>
		/// Calls shell to get users for a netgroup by calling getent
		/// netgroup, this is a low level function that just returns string
		/// that
		/// </summary>
		/// <param name="netgroup">get users for this netgroup</param>
		/// <returns>string of users for a given netgroup in getent netgroups format</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string execShellGetUserForNetgroup(string netgroup)
		{
			string result = string.Empty;
			try
			{
				// shell command does not expect '@' at the begining of the group name
				result = org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getUsersForNetgroupCommand
					(Sharpen.Runtime.substring(netgroup, 1)));
			}
			catch (org.apache.hadoop.util.Shell.ExitCodeException e)
			{
				// if we didn't get the group - just return empty list;
				LOG.warn("error getting users for netgroup " + netgroup, e);
			}
			return result;
		}
	}
}

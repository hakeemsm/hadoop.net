using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A simple shell-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that exec's the <code>groups</code> shell command to fetch the group
	/// memberships of a given user.
	/// </summary>
	public class ShellBasedUnixGroupsNetgroupMapping : ShellBasedUnixGroupsMapping
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ShellBasedUnixGroupsNetgroupMapping
			));

		/// <summary>Get unix groups (parent) and netgroups for given user</summary>
		/// <param name="user">get groups and netgroups for this user</param>
		/// <returns>groups and netgroups for user</returns>
		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetGroups(string user)
		{
			// parent get unix groups
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
		/// <summary>Gets users for a netgroup</summary>
		/// <param name="netgroup">return users for this netgroup</param>
		/// <returns>list of users for a given netgroup</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<string> GetUsersForNetgroup(string netgroup)
		{
			IList<string> users = new List<string>();
			// returns a string similar to this:
			// group               ( , user, ) ( domain, user1, host.com )
			string usersRaw = ExecShellGetUserForNetgroup(netgroup);
			// get rid of spaces, makes splitting much easier
			usersRaw = usersRaw.ReplaceAll(" +", string.Empty);
			// remove netgroup name at the beginning of the string
			usersRaw = usersRaw.ReplaceFirst(netgroup.ReplaceFirst("@", string.Empty) + "[()]+"
				, string.Empty);
			// split string into user infos
			string[] userInfos = usersRaw.Split("[()]+");
			foreach (string userInfo in userInfos)
			{
				// userInfo: xxx,user,yyy (xxx, yyy can be empty strings)
				// get rid of everything before first and after last comma
				string user = userInfo.ReplaceFirst("[^,]*,", string.Empty);
				user = user.ReplaceFirst(",.*$", string.Empty);
				// voila! got username!
				users.AddItem(user);
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
		protected internal virtual string ExecShellGetUserForNetgroup(string netgroup)
		{
			string result = string.Empty;
			try
			{
				// shell command does not expect '@' at the begining of the group name
				result = Shell.ExecCommand(Shell.GetUsersForNetgroupCommand(Runtime.Substring
					(netgroup, 1)));
			}
			catch (Shell.ExitCodeException e)
			{
				// if we didn't get the group - just return empty list;
				Log.Warn("error getting users for netgroup " + netgroup, e);
			}
			return result;
		}
	}
}

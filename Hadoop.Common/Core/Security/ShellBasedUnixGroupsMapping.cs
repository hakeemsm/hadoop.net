using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A simple shell-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that exec's the <code>groups</code> shell command to fetch the group
	/// memberships of a given user.
	/// </summary>
	public class ShellBasedUnixGroupsMapping : GroupMappingServiceProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ShellBasedUnixGroupsMapping
			));

		/// <summary>Returns list of groups for a user</summary>
		/// <param name="user">get groups for this user</param>
		/// <returns>list of groups for a given user</returns>
		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetGroups(string user)
		{
			return GetUnixGroups(user);
		}

		/// <summary>Caches groups, no need to do that for this provider</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsRefresh()
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <summary>Adds groups to cache, no need to do that for this provider</summary>
		/// <param name="groups">unused</param>
		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsAdd(IList<string> groups)
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <summary>
		/// Get the current user's group list from Unix by running the command 'groups'
		/// NOTE.
		/// </summary>
		/// <remarks>
		/// Get the current user's group list from Unix by running the command 'groups'
		/// NOTE. For non-existing user it will return EMPTY list
		/// </remarks>
		/// <param name="user">user name</param>
		/// <returns>
		/// the groups list that the <code>user</code> belongs to. The primary
		/// group is returned first.
		/// </returns>
		/// <exception cref="System.IO.IOException">if encounter any error when running the command
		/// 	</exception>
		private static IList<string> GetUnixGroups(string user)
		{
			string result = string.Empty;
			try
			{
				result = Shell.ExecCommand(Shell.GetGroupsForUserCommand(user));
			}
			catch (Shell.ExitCodeException e)
			{
				// if we didn't get the group - just return empty list;
				Log.Warn("got exception trying to get groups for user " + user + ": " + e.Message
					);
				return new List<string>();
			}
			StringTokenizer tokenizer = new StringTokenizer(result, Shell.TokenSeparatorRegex
				);
			IList<string> groups = new List<string>();
			while (tokenizer.HasMoreTokens())
			{
				groups.AddItem(tokenizer.NextToken());
			}
			// remove duplicated primary group
			if (!Shell.Windows)
			{
				for (int i = 1; i < groups.Count; i++)
				{
					if (groups[i].Equals(groups[0]))
					{
						groups.Remove(i);
						break;
					}
				}
			}
			return groups;
		}
	}
}

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
	public class ShellBasedUnixGroupsMapping : org.apache.hadoop.security.GroupMappingServiceProvider
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.ShellBasedUnixGroupsMapping
			)));

		/// <summary>Returns list of groups for a user</summary>
		/// <param name="user">get groups for this user</param>
		/// <returns>list of groups for a given user</returns>
		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getGroups(string user)
		{
			return getUnixGroups(user);
		}

		/// <summary>Caches groups, no need to do that for this provider</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsRefresh()
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <summary>Adds groups to cache, no need to do that for this provider</summary>
		/// <param name="groups">unused</param>
		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			)
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
		private static System.Collections.Generic.IList<string> getUnixGroups(string user
			)
		{
			string result = string.Empty;
			try
			{
				result = org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getGroupsForUserCommand
					(user));
			}
			catch (org.apache.hadoop.util.Shell.ExitCodeException e)
			{
				// if we didn't get the group - just return empty list;
				LOG.warn("got exception trying to get groups for user " + user + ": " + e.Message
					);
				return new System.Collections.Generic.LinkedList<string>();
			}
			java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(result, org.apache.hadoop.util.Shell
				.TOKEN_SEPARATOR_REGEX);
			System.Collections.Generic.IList<string> groups = new System.Collections.Generic.LinkedList
				<string>();
			while (tokenizer.hasMoreTokens())
			{
				groups.add(tokenizer.nextToken());
			}
			// remove duplicated primary group
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				for (int i = 1; i < groups.Count; i++)
				{
					if (groups[i].Equals(groups[0]))
					{
						groups.remove(i);
						break;
					}
				}
			}
			return groups;
		}
	}
}

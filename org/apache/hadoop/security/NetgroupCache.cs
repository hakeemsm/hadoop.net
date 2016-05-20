using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// Class that caches the netgroups and inverts group-to-user map
	/// to user-to-group map, primarily intended for use with
	/// netgroups (as returned by getent netgrgoup) which only returns
	/// group to user mapping.
	/// </summary>
	public class NetgroupCache
	{
		private static java.util.concurrent.ConcurrentHashMap<string, System.Collections.Generic.ICollection
			<string>> userToNetgroupsMap = new java.util.concurrent.ConcurrentHashMap<string
			, System.Collections.Generic.ICollection<string>>();

		/// <summary>Get netgroups for a given user</summary>
		/// <param name="user">get groups for this user</param>
		/// <param name="groups">put groups into this List</param>
		public static void getNetgroups(string user, System.Collections.Generic.IList<string
			> groups)
		{
			System.Collections.Generic.ICollection<string> userGroups = userToNetgroupsMap[user
				];
			//ConcurrentHashMap does not allow null values; 
			//So null value check can be used to check if the key exists
			if (userGroups != null)
			{
				Sharpen.Collections.AddAll(groups, userGroups);
			}
		}

		/// <summary>Get the list of cached netgroups</summary>
		/// <returns>list of cached groups</returns>
		public static System.Collections.Generic.IList<string> getNetgroupNames()
		{
			return new System.Collections.Generic.LinkedList<string>(getGroups());
		}

		private static System.Collections.Generic.ICollection<string> getGroups()
		{
			System.Collections.Generic.ICollection<string> allGroups = new java.util.HashSet<
				string>();
			foreach (System.Collections.Generic.ICollection<string> userGroups in userToNetgroupsMap
				.Values)
			{
				Sharpen.Collections.AddAll(allGroups, userGroups);
			}
			return allGroups;
		}

		/// <summary>Returns true if a given netgroup is cached</summary>
		/// <param name="group">check if this group is cached</param>
		/// <returns>true if group is cached, false otherwise</returns>
		public static bool isCached(string group)
		{
			return getGroups().contains(group);
		}

		/// <summary>Clear the cache</summary>
		public static void clear()
		{
			userToNetgroupsMap.clear();
		}

		/// <summary>Add group to cache</summary>
		/// <param name="group">name of the group to add to cache</param>
		/// <param name="users">list of users for a given group</param>
		public static void add(string group, System.Collections.Generic.IList<string> users
			)
		{
			foreach (string user in users)
			{
				System.Collections.Generic.ICollection<string> userGroups = userToNetgroupsMap[user
					];
				// ConcurrentHashMap does not allow null values; 
				// So null value check can be used to check if the key exists
				if (userGroups == null)
				{
					//Generate a ConcurrentHashSet (backed by the keyset of the ConcurrentHashMap)
					userGroups = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap
						<string, bool>());
					System.Collections.Generic.ICollection<string> currentSet = userToNetgroupsMap.putIfAbsent
						(user, userGroups);
					if (currentSet != null)
					{
						userGroups = currentSet;
					}
				}
				userGroups.add(group);
			}
		}
	}
}

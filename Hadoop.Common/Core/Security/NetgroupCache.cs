using System.Collections.Generic;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// Class that caches the netgroups and inverts group-to-user map
	/// to user-to-group map, primarily intended for use with
	/// netgroups (as returned by getent netgrgoup) which only returns
	/// group to user mapping.
	/// </summary>
	public class NetgroupCache
	{
		private static ConcurrentHashMap<string, ICollection<string>> userToNetgroupsMap = 
			new ConcurrentHashMap<string, ICollection<string>>();

		/// <summary>Get netgroups for a given user</summary>
		/// <param name="user">get groups for this user</param>
		/// <param name="groups">put groups into this List</param>
		public static void GetNetgroups(string user, IList<string> groups)
		{
			ICollection<string> userGroups = userToNetgroupsMap[user];
			//ConcurrentHashMap does not allow null values; 
			//So null value check can be used to check if the key exists
			if (userGroups != null)
			{
				Collections.AddAll(groups, userGroups);
			}
		}

		/// <summary>Get the list of cached netgroups</summary>
		/// <returns>list of cached groups</returns>
		public static IList<string> GetNetgroupNames()
		{
			return new List<string>(GetGroups());
		}

		private static ICollection<string> GetGroups()
		{
			ICollection<string> allGroups = new HashSet<string>();
			foreach (ICollection<string> userGroups in userToNetgroupsMap.Values)
			{
				Collections.AddAll(allGroups, userGroups);
			}
			return allGroups;
		}

		/// <summary>Returns true if a given netgroup is cached</summary>
		/// <param name="group">check if this group is cached</param>
		/// <returns>true if group is cached, false otherwise</returns>
		public static bool IsCached(string group)
		{
			return GetGroups().Contains(group);
		}

		/// <summary>Clear the cache</summary>
		public static void Clear()
		{
			userToNetgroupsMap.Clear();
		}

		/// <summary>Add group to cache</summary>
		/// <param name="group">name of the group to add to cache</param>
		/// <param name="users">list of users for a given group</param>
		public static void Add(string group, IList<string> users)
		{
			foreach (string user in users)
			{
				ICollection<string> userGroups = userToNetgroupsMap[user];
				// ConcurrentHashMap does not allow null values; 
				// So null value check can be used to check if the key exists
				if (userGroups == null)
				{
					//Generate a ConcurrentHashSet (backed by the keyset of the ConcurrentHashMap)
					userGroups = Collections.NewSetFromMap(new ConcurrentHashMap<string, bool
						>());
					ICollection<string> currentSet = userToNetgroupsMap.PutIfAbsent(user, userGroups);
					if (currentSet != null)
					{
						userGroups = currentSet;
					}
				}
				userGroups.AddItem(group);
			}
		}
	}
}

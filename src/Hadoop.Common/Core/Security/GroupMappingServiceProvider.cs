using System.Collections.Generic;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// An interface for the implementation of a user-to-groups mapping service
	/// used by
	/// <see cref="Groups"/>
	/// .
	/// </summary>
	public abstract class GroupMappingServiceProvider
	{
		public const string GroupMappingConfigPrefix = CommonConfigurationKeysPublic.HadoopSecurityGroupMapping;

		/// <summary>Get all various group memberships of a given user.</summary>
		/// <remarks>
		/// Get all various group memberships of a given user.
		/// Returns EMPTY list in case of non-existing user
		/// </remarks>
		/// <param name="user">User's name</param>
		/// <returns>group memberships of user</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<string> GetGroups(string user);

		/// <summary>Refresh the cache of groups and user mapping</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void CacheGroupsRefresh();

		/// <summary>Caches the group user information</summary>
		/// <param name="groups">list of groups to add to cache</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void CacheGroupsAdd(IList<string> groups);
	}

	public static class GroupMappingServiceProviderConstants
	{
	}
}

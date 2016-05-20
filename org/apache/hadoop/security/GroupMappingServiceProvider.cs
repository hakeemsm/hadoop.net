using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// An interface for the implementation of a user-to-groups mapping service
	/// used by
	/// <see cref="Groups"/>
	/// .
	/// </summary>
	public abstract class GroupMappingServiceProvider
	{
		public const string GROUP_MAPPING_CONFIG_PREFIX = org.apache.hadoop.fs.CommonConfigurationKeysPublic
			.HADOOP_SECURITY_GROUP_MAPPING;

		/// <summary>Get all various group memberships of a given user.</summary>
		/// <remarks>
		/// Get all various group memberships of a given user.
		/// Returns EMPTY list in case of non-existing user
		/// </remarks>
		/// <param name="user">User's name</param>
		/// <returns>group memberships of user</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract System.Collections.Generic.IList<string> getGroups(string user);

		/// <summary>Refresh the cache of groups and user mapping</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void cacheGroupsRefresh();

		/// <summary>Caches the group user information</summary>
		/// <param name="groups">list of groups to add to cache</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			);
	}

	public static class GroupMappingServiceProviderConstants
	{
	}
}

using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// An interface for the implementation of <userId, userName> mapping
	/// and <groupId, groupName> mapping
	/// </summary>
	public interface IdMappingServiceProvider
	{
		// Return uid for given user name
		/// <exception cref="System.IO.IOException"/>
		int getUid(string user);

		// Return gid for given group name
		/// <exception cref="System.IO.IOException"/>
		int getGid(string group);

		// Return user name for given user id uid, if not found, return 
		// <unknown> passed to this method
		string getUserName(int uid, string unknown);

		// Return group name for given groupd id gid, if not found, return 
		// <unknown> passed to this method
		string getGroupName(int gid, string unknown);

		// Return uid for given user name.
		// When can't map user, return user name's string hashcode
		int getUidAllowingUnknown(string user);

		// Return gid for given group name.
		// When can't map group, return group name's string hashcode
		int getGidAllowingUnknown(string group);
	}
}

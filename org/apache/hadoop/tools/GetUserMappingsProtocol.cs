using Sharpen;

namespace org.apache.hadoop.tools
{
	/// <summary>
	/// Protocol implemented by the Name Node and Job Tracker which maps users to
	/// groups.
	/// </summary>
	public abstract class GetUserMappingsProtocol
	{
		/// <summary>Version 1: Initial version.</summary>
		public const long versionID = 1L;

		/// <summary>Get the groups which are mapped to the given user.</summary>
		/// <param name="user">The user to get the groups for.</param>
		/// <returns>The set of groups the user belongs to.</returns>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract string[] getGroupsForUser(string user);
	}

	public static class GetUserMappingsProtocolConstants
	{
	}
}

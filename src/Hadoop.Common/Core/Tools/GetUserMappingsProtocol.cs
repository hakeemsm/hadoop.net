using Org.Apache.Hadoop.IO.Retry;


namespace Org.Apache.Hadoop.Tools
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
		[Idempotent]
		public abstract string[] GetGroupsForUser(string user);
	}

	public static class GetUserMappingsProtocolConstants
	{
	}
}

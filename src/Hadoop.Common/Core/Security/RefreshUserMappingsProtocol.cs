using Org.Apache.Hadoop.IO.Retry;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>Protocol use</summary>
	public abstract class RefreshUserMappingsProtocol
	{
		/// <summary>Version 1: Initial version.</summary>
		public const long versionID = 1L;

		/// <summary>Refresh user to group mappings.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RefreshUserToGroupsMappings();

		/// <summary>Refresh superuser proxy group list</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RefreshSuperUserGroupsConfiguration();
	}

	public static class RefreshUserMappingsProtocolConstants
	{
	}
}

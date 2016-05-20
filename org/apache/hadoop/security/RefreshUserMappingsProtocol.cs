using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>Protocol use</summary>
	public abstract class RefreshUserMappingsProtocol
	{
		/// <summary>Version 1: Initial version.</summary>
		public const long versionID = 1L;

		/// <summary>Refresh user to group mappings.</summary>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void refreshUserToGroupsMappings();

		/// <summary>Refresh superuser proxy group list</summary>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void refreshSuperUserGroupsConfiguration();
	}

	public static class RefreshUserMappingsProtocolConstants
	{
	}
}

using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	/// <summary>Protocol which is used to refresh the authorization policy in use currently.
	/// 	</summary>
	public abstract class RefreshAuthorizationPolicyProtocol
	{
		/// <summary>Version 1: Initial version</summary>
		public const long versionID = 1L;

		/// <summary>Refresh the service-level authorization policy in-effect.</summary>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void refreshServiceAcl();
	}

	public static class RefreshAuthorizationPolicyProtocolConstants
	{
	}
}

using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Protocol which is used to refresh arbitrary things at runtime.</summary>
	public abstract class GenericRefreshProtocol
	{
		/// <summary>Version 1: Initial version.</summary>
		public const long versionID = 1L;

		/// <summary>Refresh the resource based on identity passed in.</summary>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshResponse
			> refresh(string identifier, string[] args);
	}

	public static class GenericRefreshProtocolConstants
	{
	}
}

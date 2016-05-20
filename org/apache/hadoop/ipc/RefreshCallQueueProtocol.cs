using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Protocol which is used to refresh the call queue in use currently.</summary>
	public abstract class RefreshCallQueueProtocol
	{
		/// <summary>Version 1: Initial version</summary>
		public const long versionID = 1L;

		/// <summary>Refresh the callqueue.</summary>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void refreshCallQueue();
	}

	public static class RefreshCallQueueProtocolConstants
	{
	}
}

using Org.Apache.Hadoop.IO.Retry;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Protocol which is used to refresh the call queue in use currently.</summary>
	public abstract class RefreshCallQueueProtocol
	{
		/// <summary>Version 1: Initial version</summary>
		public const long versionID = 1L;

		/// <summary>Refresh the callqueue.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void RefreshCallQueue();
	}

	public static class RefreshCallQueueProtocolConstants
	{
	}
}

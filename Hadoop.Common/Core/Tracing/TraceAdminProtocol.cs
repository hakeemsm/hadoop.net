using Org.Apache.Hadoop.IO.Retry;


namespace Org.Apache.Hadoop.Tracing
{
	/// <summary>Protocol interface that provides tracing.</summary>
	public abstract class TraceAdminProtocol
	{
		public const long versionID = 1L;

		/// <summary>List the currently active trace span receivers.</summary>
		/// <exception cref="System.IO.IOException">On error.</exception>
		[Idempotent]
		public abstract SpanReceiverInfo[] ListSpanReceivers();

		/// <summary>Add a new trace span receiver.</summary>
		/// <param name="desc">The span receiver description.</param>
		/// <returns>The ID of the new trace span receiver.</returns>
		/// <exception cref="System.IO.IOException">On error.</exception>
		[AtMostOnce]
		public abstract long AddSpanReceiver(SpanReceiverInfo desc);

		/// <summary>Remove a trace span receiver.</summary>
		/// <param name="spanReceiverId">The id of the span receiver to remove.</param>
		/// <exception cref="System.IO.IOException">On error.</exception>
		[AtMostOnce]
		public abstract void RemoveSpanReceiver(long spanReceiverId);
	}

	public static class TraceAdminProtocolConstants
	{
	}
}

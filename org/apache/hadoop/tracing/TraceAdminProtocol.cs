using Sharpen;

namespace org.apache.hadoop.tracing
{
	/// <summary>Protocol interface that provides tracing.</summary>
	public abstract class TraceAdminProtocol
	{
		public const long versionID = 1L;

		/// <summary>List the currently active trace span receivers.</summary>
		/// <exception cref="System.IO.IOException">On error.</exception>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract org.apache.hadoop.tracing.SpanReceiverInfo[] listSpanReceivers();

		/// <summary>Add a new trace span receiver.</summary>
		/// <param name="desc">The span receiver description.</param>
		/// <returns>The ID of the new trace span receiver.</returns>
		/// <exception cref="System.IO.IOException">On error.</exception>
		[org.apache.hadoop.io.retry.AtMostOnce]
		public abstract long addSpanReceiver(org.apache.hadoop.tracing.SpanReceiverInfo desc
			);

		/// <summary>Remove a trace span receiver.</summary>
		/// <param name="spanReceiverId">The id of the span receiver to remove.</param>
		/// <exception cref="System.IO.IOException">On error.</exception>
		[org.apache.hadoop.io.retry.AtMostOnce]
		public abstract void removeSpanReceiver(long spanReceiverId);
	}

	public static class TraceAdminProtocolConstants
	{
	}
}

using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Implement this interface to make a pluggable multiplexer in the
	/// FairCallQueue.
	/// </summary>
	public interface RpcMultiplexer
	{
		/// <summary>
		/// Should get current index and optionally perform whatever is needed
		/// to prepare the next index.
		/// </summary>
		/// <returns>current index</returns>
		int getAndAdvanceCurrentIndex();
	}
}

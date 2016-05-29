using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response to admin from the <code>SharedCacheManager</code> when
	/// is asked to run the cleaner service.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response to admin from the <code>SharedCacheManager</code> when
	/// is asked to run the cleaner service.
	/// </p>
	/// <p>
	/// Currently, this is empty.
	/// </p>
	/// </remarks>
	public abstract class RunSharedCacheCleanerTaskResponse
	{
		/// <summary>Get whether or not the shared cache manager has accepted the request.</summary>
		/// <remarks>
		/// Get whether or not the shared cache manager has accepted the request.
		/// Shared cache manager will reject the request if there is an ongoing task
		/// </remarks>
		/// <returns>boolean True if the request has been accepted, false otherwise.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract bool GetAccepted();

		/// <summary>
		/// Set whether or not the shared cache manager has accepted the request Shared
		/// cache manager will reject the request if there is an ongoing task
		/// </summary>
		/// <param name="b">True if the request has been accepted, false otherwise.</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAccepted(bool b);
	}
}

using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response from the SharedCacheManager to the NodeManager that indicates
	/// whether the NodeManager needs to delete the cached resource it was sending
	/// the notification for.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response from the SharedCacheManager to the NodeManager that indicates
	/// whether the NodeManager needs to delete the cached resource it was sending
	/// the notification for.
	/// </p>
	/// </remarks>
	public abstract class SCMUploaderNotifyResponse
	{
		/// <summary>
		/// Get whether or not the shared cache manager has accepted the notified
		/// resource (i.e.
		/// </summary>
		/// <remarks>
		/// Get whether or not the shared cache manager has accepted the notified
		/// resource (i.e. the uploaded file should remain in the cache).
		/// </remarks>
		/// <returns>boolean True if the resource has been accepted, false otherwise.</returns>
		public abstract bool GetAccepted();

		/// <summary>
		/// Set whether or not the shared cache manager has accepted the notified
		/// resource (i.e.
		/// </summary>
		/// <remarks>
		/// Set whether or not the shared cache manager has accepted the notified
		/// resource (i.e. the uploaded file should remain in the cache).
		/// </remarks>
		/// <param name="b">True if the resource has been accepted, false otherwise.</param>
		public abstract void SetAccepted(bool b);
	}
}

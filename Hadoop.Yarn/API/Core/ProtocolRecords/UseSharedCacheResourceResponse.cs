using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response from the SharedCacheManager to the client that indicates whether
	/// a requested resource exists in the cache.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response from the SharedCacheManager to the client that indicates whether
	/// a requested resource exists in the cache.
	/// </p>
	/// </remarks>
	public abstract class UseSharedCacheResourceResponse
	{
		/// <summary>
		/// Get the <code>Path</code> corresponding to the requested resource in the
		/// shared cache.
		/// </summary>
		/// <returns>
		/// String A <code>Path</code> if the resource exists in the shared
		/// cache, <code>null</code> otherwise
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetPath();

		/// <summary>Set the <code>Path</code> corresponding to a resource in the shared cache.
		/// 	</summary>
		/// <param name="p">
		/// A <code>Path</code> corresponding to a resource in the shared
		/// cache
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetPath(string p);
	}
}

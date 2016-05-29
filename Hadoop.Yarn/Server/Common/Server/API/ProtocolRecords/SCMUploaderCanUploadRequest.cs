using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request from the NodeManager to the <code>SharedCacheManager</code> that
	/// requests whether it can upload a resource in the shared cache.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The request from the NodeManager to the <code>SharedCacheManager</code> that
	/// requests whether it can upload a resource in the shared cache.
	/// </p>
	/// </remarks>
	public abstract class SCMUploaderCanUploadRequest
	{
		/// <summary>
		/// Get the <code>key</code> of the resource that would be uploaded to the
		/// shared cache.
		/// </summary>
		/// <returns><code>key</code></returns>
		public abstract string GetResourceKey();

		/// <summary>
		/// Set the <code>key</code> of the resource that would be uploaded to the
		/// shared cache.
		/// </summary>
		/// <param name="key">unique identifier for the resource</param>
		public abstract void SetResourceKey(string key);
	}
}

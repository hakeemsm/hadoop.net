using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request from the NodeManager to the <code>SharedCacheManager</code> that
	/// notifies that a resource has been uploaded to the shared cache.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The request from the NodeManager to the <code>SharedCacheManager</code> that
	/// notifies that a resource has been uploaded to the shared cache. The
	/// <code>SharedCacheManager</code> may reject the resource for various reasons,
	/// in which case the NodeManager should remove it from the shared cache.
	/// </p>
	/// </remarks>
	public abstract class SCMUploaderNotifyRequest
	{
		/// <summary>
		/// Get the filename of the resource that was just uploaded to the shared
		/// cache.
		/// </summary>
		/// <returns>the filename</returns>
		public abstract string GetFileName();

		/// <summary>
		/// Set the filename of the resource that was just uploaded to the shared
		/// cache.
		/// </summary>
		/// <param name="filename">the name of the file</param>
		public abstract void SetFilename(string filename);

		/// <summary>
		/// Get the <code>key</code> of the resource that was just uploaded to the
		/// shared cache.
		/// </summary>
		/// <returns><code>key</code></returns>
		public abstract string GetResourceKey();

		/// <summary>
		/// Set the <code>key</code> of the resource that was just uploaded to the
		/// shared cache.
		/// </summary>
		/// <param name="key">unique identifier for the resource</param>
		public abstract void SetResourceKey(string key);
	}
}

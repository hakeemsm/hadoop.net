using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response from the SharedCacheManager to the NodeManager that indicates
	/// whether the NodeManager can upload the resource to the shared cache.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response from the SharedCacheManager to the NodeManager that indicates
	/// whether the NodeManager can upload the resource to the shared cache. If it is
	/// not accepted by SCM, the NodeManager should not upload it to the shared
	/// cache.
	/// </p>
	/// </remarks>
	public abstract class SCMUploaderCanUploadResponse
	{
		/// <summary>
		/// Get whether or not the node manager can upload the resource to the shared
		/// cache.
		/// </summary>
		/// <returns>boolean True if the resource can be uploaded, false otherwise.</returns>
		public abstract bool GetUploadable();

		/// <summary>
		/// Set whether or not the node manager can upload the resource to the shared
		/// cache.
		/// </summary>
		/// <param name="b">True if the resource can be uploaded, false otherwise.</param>
		public abstract void SetUploadable(bool b);
	}
}

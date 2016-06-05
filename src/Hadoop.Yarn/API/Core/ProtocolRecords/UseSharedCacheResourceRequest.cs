using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request from clients to the <code>SharedCacheManager</code> that claims a
	/// resource in the shared cache.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The request from clients to the <code>SharedCacheManager</code> that claims a
	/// resource in the shared cache.
	/// </p>
	/// </remarks>
	public abstract class UseSharedCacheResourceRequest
	{
		/// <summary>Get the <code>ApplicationId</code> of the resource to be used.</summary>
		/// <returns><code>ApplicationId</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationId GetAppId();

		/// <summary>Set the <code>ApplicationId</code> of the resource to be used.</summary>
		/// <param name="id"><code>ApplicationId</code></param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAppId(ApplicationId id);

		/// <summary>Get the <code>key</code> of the resource to be used.</summary>
		/// <returns><code>key</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetResourceKey();

		/// <summary>Set the <code>key</code> of the resource to be used.</summary>
		/// <param name="key">unique identifier for the resource</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetResourceKey(string key);
	}
}

using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary><p>The request from clients to release a resource in the shared cache.</p>
	/// 	</summary>
	public abstract class ReleaseSharedCacheResourceRequest
	{
		/// <summary>Get the <code>ApplicationId</code> of the resource to be released.</summary>
		/// <returns><code>ApplicationId</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationId GetAppId();

		/// <summary>Set the <code>ApplicationId</code> of the resource to be released.</summary>
		/// <param name="id"><code>ApplicationId</code></param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAppId(ApplicationId id);

		/// <summary>Get the <code>key</code> of the resource to be released.</summary>
		/// <returns><code>key</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetResourceKey();

		/// <summary>Set the <code>key</code> of the resource to be released.</summary>
		/// <param name="key">unique identifier for the resource</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetResourceKey(string key);
	}
}

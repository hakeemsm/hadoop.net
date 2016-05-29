using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// <p>
	/// The protocol between clients and the <code>SharedCacheManager</code> to claim
	/// and release resources in the shared cache.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The protocol between clients and the <code>SharedCacheManager</code> to claim
	/// and release resources in the shared cache.
	/// </p>
	/// </remarks>
	public interface ClientSCMProtocol
	{
		/// <summary>
		/// <p>
		/// The interface used by clients to claim a resource with the
		/// <code>SharedCacheManager.</code> The client uses a checksum to identify the
		/// resource and an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// to identify which application will be
		/// using the resource.
		/// </p>
		/// <p>
		/// The <code>SharedCacheManager</code> responds with whether or not the
		/// resource exists in the cache. If the resource exists, a <code>Path</code>
		/// to the resource in the shared cache is returned. If the resource does not
		/// exist, the response is empty.
		/// </p>
		/// </summary>
		/// <param name="request">request to claim a resource in the shared cache</param>
		/// <returns>response indicating if the resource is already in the cache</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		UseSharedCacheResourceResponse Use(UseSharedCacheResourceRequest request);

		/// <summary>
		/// <p>
		/// The interface used by clients to release a resource with the
		/// <code>SharedCacheManager.</code> This method is called once an application
		/// is no longer using a claimed resource in the shared cache.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The interface used by clients to release a resource with the
		/// <code>SharedCacheManager.</code> This method is called once an application
		/// is no longer using a claimed resource in the shared cache. The client uses
		/// a checksum to identify the resource and an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// to
		/// identify which application is releasing the resource.
		/// </p>
		/// <p>
		/// Note: This method is an optimization and the client is not required to call
		/// it for correctness.
		/// </p>
		/// <p>
		/// Currently the <code>SharedCacheManager</code> sends an empty response.
		/// </p>
		/// </remarks>
		/// <param name="request">request to release a resource in the shared cache</param>
		/// <returns>(empty) response on releasing the resource</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		ReleaseSharedCacheResourceResponse Release(ReleaseSharedCacheResourceRequest request
			);
	}
}

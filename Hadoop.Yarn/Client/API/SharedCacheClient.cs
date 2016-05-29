using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	/// <summary>This is the client for YARN's shared cache.</summary>
	public abstract class SharedCacheClient : AbstractService
	{
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.SharedCacheClient CreateSharedCacheClient
			()
		{
			Org.Apache.Hadoop.Yarn.Client.Api.SharedCacheClient client = new SharedCacheClientImpl
				();
			return client;
		}

		[InterfaceAudience.Private]
		public SharedCacheClient(string name)
			: base(name)
		{
		}

		/// <summary>
		/// <p>
		/// The method to claim a resource with the <code>SharedCacheManager.</code>
		/// The client uses a checksum to identify the resource and an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// to identify which application will be using the
		/// resource.
		/// </p>
		/// <p>
		/// The <code>SharedCacheManager</code> responds with whether or not the
		/// resource exists in the cache. If the resource exists, a <code>Path</code>
		/// to the resource in the shared cache is returned. If the resource does not
		/// exist, null is returned instead.
		/// </p>
		/// </summary>
		/// <param name="applicationId">ApplicationId of the application using the resource</param>
		/// <param name="resourceKey">the key (i.e. checksum) that identifies the resource</param>
		/// <returns>Path to the resource, or null if it does not exist</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Path Use(ApplicationId applicationId, string resourceKey);

		/// <summary>
		/// <p>
		/// The method to release a resource with the <code>SharedCacheManager.</code>
		/// This method is called once an application is no longer using a claimed
		/// resource in the shared cache.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The method to release a resource with the <code>SharedCacheManager.</code>
		/// This method is called once an application is no longer using a claimed
		/// resource in the shared cache. The client uses a checksum to identify the
		/// resource and an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// to identify which application is
		/// releasing the resource.
		/// </p>
		/// <p>
		/// Note: This method is an optimization and the client is not required to call
		/// it for correctness.
		/// </p>
		/// </remarks>
		/// <param name="applicationId">
		/// ApplicationId of the application releasing the
		/// resource
		/// </param>
		/// <param name="resourceKey">the key (i.e. checksum) that identifies the resource</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void Release(ApplicationId applicationId, string resourceKey);

		/// <summary>A convenience method to calculate the checksum of a specified file.</summary>
		/// <param name="sourceFile">A path to the input file</param>
		/// <returns>A hex string containing the checksum digest</returns>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetFileChecksum(Path sourceFile);
	}
}

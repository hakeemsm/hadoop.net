using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// <p>
	/// The protocol between administrators and the <code>SharedCacheManager</code>
	/// </p>
	/// </summary>
	public interface SCMAdminProtocol
	{
		/// <summary>
		/// <p>
		/// The method used by administrators to ask SCM to run cleaner task right away
		/// </p>
		/// </summary>
		/// <param name="request">request <code>SharedCacheManager</code> to run a cleaner task
		/// 	</param>
		/// <returns>
		/// <code>SharedCacheManager</code> returns an empty response
		/// on success and throws an exception on rejecting the request
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		RunSharedCacheCleanerTaskResponse RunCleanerTask(RunSharedCacheCleanerTaskRequest
			 request);
	}
}

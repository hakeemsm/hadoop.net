using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// <p>
	/// The protocol between a <code>NodeManager's</code>
	/// <code>SharedCacheUploadService</code> and the
	/// <code>SharedCacheManager.</code>
	/// </p>
	/// </summary>
	public interface SCMUploaderProtocol
	{
		/// <summary>
		/// <p>
		/// The method used by the NodeManager's <code>SharedCacheUploadService</code>
		/// to notify the shared cache manager of a newly cached resource.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The method used by the NodeManager's <code>SharedCacheUploadService</code>
		/// to notify the shared cache manager of a newly cached resource.
		/// </p>
		/// <p>
		/// The <code>SharedCacheManager</code> responds with whether or not the
		/// NodeManager should delete the uploaded file.
		/// </p>
		/// </remarks>
		/// <param name="request">
		/// notify the shared cache manager of a newly uploaded resource
		/// to the shared cache
		/// </param>
		/// <returns>
		/// response indicating if the newly uploaded resource should be
		/// deleted
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		SCMUploaderNotifyResponse Notify(SCMUploaderNotifyRequest request);

		/// <summary>
		/// <p>
		/// The method used by the NodeManager's <code>SharedCacheUploadService</code>
		/// to request whether a resource can be uploaded.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The method used by the NodeManager's <code>SharedCacheUploadService</code>
		/// to request whether a resource can be uploaded.
		/// </p>
		/// <p>
		/// The <code>SharedCacheManager</code> responds with whether or not the
		/// NodeManager can upload the file.
		/// </p>
		/// </remarks>
		/// <param name="request">whether the resource can be uploaded to the shared cache</param>
		/// <returns>response indicating if resource can be uploaded to the shared cache</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		SCMUploaderCanUploadResponse CanUpload(SCMUploaderCanUploadRequest request);
	}
}

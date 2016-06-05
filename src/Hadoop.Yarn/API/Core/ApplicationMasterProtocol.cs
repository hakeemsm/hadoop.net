using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// <p>The protocol between a live instance of <code>ApplicationMaster</code>
	/// and the <code>ResourceManager</code>.</p>
	/// <p>This is used by the <code>ApplicationMaster</code> to register/unregister
	/// and to request and obtain resources in the cluster from the
	/// <code>ResourceManager</code>.</p>
	/// </summary>
	public interface ApplicationMasterProtocol
	{
		/// <summary>
		/// <p>
		/// The interface used by a new <code>ApplicationMaster</code> to register with
		/// the <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The interface used by a new <code>ApplicationMaster</code> to register with
		/// the <code>ResourceManager</code>.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> needs to provide details such as RPC
		/// Port, HTTP tracking url etc. as specified in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterRequest
		/// 	"/>
		/// .
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code> responds with critical details such as
		/// maximum resource capabilities in the cluster as specified in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterResponse
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <param name="request">registration request</param>
		/// <returns>registration respose</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidApplicationMasterRequestException
		/// 	">
		/// The exception is thrown when an ApplicationMaster tries to
		/// register more then once.
		/// </exception>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterRequest
		/// 	"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterResponse
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
			 request);

		/// <summary>
		/// <p>The interface used by an <code>ApplicationMaster</code> to notify the
		/// <code>ResourceManager</code> about its completion (success or failed).</p>
		/// <p>The <code>ApplicationMaster</code> has to provide details such as
		/// final state, diagnostics (in case of failures) etc.
		/// </summary>
		/// <remarks>
		/// <p>The interface used by an <code>ApplicationMaster</code> to notify the
		/// <code>ResourceManager</code> about its completion (success or failed).</p>
		/// <p>The <code>ApplicationMaster</code> has to provide details such as
		/// final state, diagnostics (in case of failures) etc. as specified in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterRequest
		/// 	"/>
		/// .</p>
		/// <p>The <code>ResourceManager</code> responds with
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterResponse
		/// 	"/>
		/// .</p>
		/// </remarks>
		/// <param name="request">completion request</param>
		/// <returns>completion response</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterRequest
		/// 	"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterResponse
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[AtMostOnce]
		FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
			 request);

		/// <summary>
		/// <p>
		/// The main interface between an <code>ApplicationMaster</code> and the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The main interface between an <code>ApplicationMaster</code> and the
		/// <code>ResourceManager</code>.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> uses this interface to provide a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
		/// and returns unused
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// allocated to
		/// it via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest"/>
		/// . Optionally, the
		/// <code>ApplicationMaster</code> can also <em>blacklist</em> resources which
		/// it doesn't want to use.
		/// </p>
		/// <p>
		/// This also doubles up as a <em>heartbeat</em> to let the
		/// <code>ResourceManager</code> know that the <code>ApplicationMaster</code>
		/// is alive. Thus, applications should periodically make this call to be kept
		/// alive. The frequency depends on
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmAmExpiryIntervalMs"/>
		/// which defaults to
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmAmExpiryIntervalMs
		/// 	"/>
		/// .
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code> responds with list of allocated
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// , status of completed containers and headroom information
		/// for the application.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> can use the available headroom
		/// (resources) to decide how to utilized allocated resources and make informed
		/// decisions about future resource requests.
		/// </p>
		/// </remarks>
		/// <param name="request">allocation request</param>
		/// <returns>allocation response</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidApplicationMasterRequestException
		/// 	">
		/// This exception is thrown when an ApplicationMaster calls allocate
		/// without registering first.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceBlacklistRequestException
		/// 	">
		/// This exception is thrown when an application provides an invalid
		/// specification for blacklist of resources.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	">
		/// This exception is thrown when a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
		/// is out of
		/// the range of the configured lower and upper limits on the
		/// resources.
		/// </exception>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateResponse"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[AtMostOnce]
		AllocateResponse Allocate(AllocateRequest request);
	}
}

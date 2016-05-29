using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request which contains a list of
	/// <see cref="StartContainerRequest"/>
	/// sent by
	/// the <code>ApplicationMaster</code> to the <code>NodeManager</code> to
	/// <em>start</em> containers.
	/// </p>
	/// <p>
	/// In each
	/// <see cref="StartContainerRequest"/>
	/// , the <code>ApplicationMaster</code> has
	/// to provide details such as allocated resource capability, security tokens (if
	/// enabled), command to be executed to start the container, environment for the
	/// process, necessary binaries/jar/shared-objects etc. via the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
	/// .
	/// </p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(StartContainersRequest)
	/// 	"/>
	public abstract class StartContainersRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static StartContainersRequest NewInstance(IList<StartContainerRequest> requests
			)
		{
			StartContainersRequest request = Records.NewRecord<StartContainersRequest>();
			request.SetStartContainerRequests(requests);
			return request;
		}

		/// <summary>
		/// Get a list of
		/// <see cref="StartContainerRequest"/>
		/// to start containers.
		/// </summary>
		/// <returns>
		/// a list of
		/// <see cref="StartContainerRequest"/>
		/// to start containers.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<StartContainerRequest> GetStartContainerRequests();

		/// <summary>
		/// Set a list of
		/// <see cref="StartContainerRequest"/>
		/// to start containers.
		/// </summary>
		/// <param name="request">
		/// a list of
		/// <see cref="StartContainerRequest"/>
		/// to start containers
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetStartContainerRequests(IList<StartContainerRequest> request
			);
	}
}

using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by the <code>ApplicationMaster</code> to the
	/// <code>NodeManager</code> to <em>start</em> a container.</p>
	/// <p>The <code>ApplicationMaster</code> has to provide details such as
	/// allocated resource capability, security tokens (if enabled), command
	/// to be executed to start the container, environment for the process,
	/// necessary binaries/jar/shared-objects etc.
	/// </summary>
	/// <remarks>
	/// <p>The request sent by the <code>ApplicationMaster</code> to the
	/// <code>NodeManager</code> to <em>start</em> a container.</p>
	/// <p>The <code>ApplicationMaster</code> has to provide details such as
	/// allocated resource capability, security tokens (if enabled), command
	/// to be executed to start the container, environment for the process,
	/// necessary binaries/jar/shared-objects etc. via the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
	/// .</p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(StartContainersRequest)
	/// 	"/>
	public abstract class StartContainerRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static StartContainerRequest NewInstance(ContainerLaunchContext context, Token
			 container)
		{
			StartContainerRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<StartContainerRequest
				>();
			request.SetContainerLaunchContext(context);
			request.SetContainerToken(container);
			return request;
		}

		/// <summary>
		/// Get the <code>ContainerLaunchContext</code> for the container to be started
		/// by the <code>NodeManager</code>.
		/// </summary>
		/// <returns>
		/// <code>ContainerLaunchContext</code> for the container to be started
		/// by the <code>NodeManager</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ContainerLaunchContext GetContainerLaunchContext();

		/// <summary>
		/// Set the <code>ContainerLaunchContext</code> for the container to be started
		/// by the <code>NodeManager</code>
		/// </summary>
		/// <param name="context">
		/// <code>ContainerLaunchContext</code> for the container to be
		/// started by the <code>NodeManager</code>
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetContainerLaunchContext(ContainerLaunchContext context);

		/// <summary>
		/// Get the container token to be used for authorization during starting
		/// container.
		/// </summary>
		/// <remarks>
		/// Get the container token to be used for authorization during starting
		/// container.
		/// <p>
		/// Note:
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NMToken"/>
		/// will be used for authenticating communication with
		/// <c>NodeManager</c>
		/// .
		/// </remarks>
		/// <returns>
		/// the container token to be used for authorization during starting
		/// container.
		/// </returns>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.NMToken"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(StartContainersRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Token GetContainerToken();

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetContainerToken(Token container);
	}
}

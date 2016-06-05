using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>A generic service that will be started by the NodeManager.</summary>
	/// <remarks>
	/// A generic service that will be started by the NodeManager. This is a service
	/// that administrators have to configure on each node by setting
	/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmAuxServices"/>
	/// .
	/// </remarks>
	public abstract class AuxiliaryService : AbstractService
	{
		private Path recoveryPath = null;

		protected internal AuxiliaryService(string name)
			: base(name)
		{
		}

		/// <summary>Get the path specific to this auxiliary service to use for recovery.</summary>
		/// <returns>state storage path or null if recovery is not enabled</returns>
		protected internal virtual Path GetRecoveryPath()
		{
			return recoveryPath;
		}

		/// <summary>A new application is started on this NodeManager.</summary>
		/// <remarks>
		/// A new application is started on this NodeManager. This is a signal to
		/// this
		/// <see cref="AuxiliaryService"/>
		/// about the application initialization.
		/// </remarks>
		/// <param name="initAppContext">context for the application's initialization</param>
		public abstract void InitializeApplication(ApplicationInitializationContext initAppContext
			);

		/// <summary>An application is finishing on this NodeManager.</summary>
		/// <remarks>
		/// An application is finishing on this NodeManager. This is a signal to this
		/// <see cref="AuxiliaryService"/>
		/// about the same.
		/// </remarks>
		/// <param name="stopAppContext">context for the application termination</param>
		public abstract void StopApplication(ApplicationTerminationContext stopAppContext
			);

		/// <summary>
		/// Retrieve meta-data for this
		/// <see cref="AuxiliaryService"/>
		/// . Applications using
		/// this
		/// <see cref="AuxiliaryService"/>
		/// SHOULD know the format of the meta-data -
		/// ideally each service should provide a method to parse out the information
		/// to the applications. One example of meta-data is contact information so
		/// that applications can access the service remotely. This will only be called
		/// after the service's
		/// <see cref="Org.Apache.Hadoop.Service.AbstractService.Start()"/>
		/// method has finished. the result may be
		/// cached.
		/// <p>
		/// The information is passed along to applications via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersResponse.GetAllServicesMetaData()
		/// 	"/>
		/// that is returned by
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
		/// 	"/>
		/// </p>
		/// </summary>
		/// <returns>
		/// meta-data for this service that should be made available to
		/// applications.
		/// </returns>
		public abstract ByteBuffer GetMetaData();

		/// <summary>A new container is started on this NodeManager.</summary>
		/// <remarks>
		/// A new container is started on this NodeManager. This is a signal to
		/// this
		/// <see cref="AuxiliaryService"/>
		/// about the container initialization.
		/// This method is called when the NodeManager receives the container launch
		/// command from the ApplicationMaster and before the container process is
		/// launched.
		/// </remarks>
		/// <param name="initContainerContext">context for the container's initialization</param>
		public virtual void InitializeContainer(ContainerInitializationContext initContainerContext
			)
		{
		}

		/// <summary>A container is finishing on this NodeManager.</summary>
		/// <remarks>
		/// A container is finishing on this NodeManager. This is a signal to this
		/// <see cref="AuxiliaryService"/>
		/// about the same.
		/// </remarks>
		/// <param name="stopContainerContext">context for the container termination</param>
		public virtual void StopContainer(ContainerTerminationContext stopContainerContext
			)
		{
		}

		/// <summary>
		/// Set the path for this auxiliary service to use for storing state
		/// that will be used during recovery.
		/// </summary>
		/// <param name="recoveryPath">where recoverable state should be stored</param>
		public virtual void SetRecoveryPath(Path recoveryPath)
		{
			this.recoveryPath = recoveryPath;
		}
	}
}
